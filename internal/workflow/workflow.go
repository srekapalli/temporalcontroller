// Package workflow defines the Temporal workflow started for each ApplicationManifest generation.
package workflow

import (
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/srekapalli/temporalcontroller/api/v1alpha1"
)

const (
	WorkflowName  = "ApplicationManifestWorkflow"
	ProgressQuery = "progress"

	defaultStepTimeout = 10 * time.Minute
)

type Input struct {
	Namespace  string                           `json:"namespace"`
	Name       string                           `json:"name"`
	UID        string                           `json:"uid"`
	Generation int64                            `json:"generation"`
	Spec       v1alpha1.ApplicationManifestSpec `json:"spec"`
}

// Result is the workflow's return value, visible in the Temporal UI and via `temporal workflow result`.
type Result struct {
	App           string            `json:"app"`
	Namespace     string            `json:"namespace"`
	Generation    int64             `json:"generation"`
	Cluster       TargetCluster     `json:"cluster"`
	Components    []ComponentResult `json:"components"`
	Notifications []SlackMessage    `json:"notifications,omitempty"`
	StartedAt     time.Time         `json:"startedAt"`
	CompletedAt   time.Time         `json:"completedAt"`
}

type ComponentResult struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Endpoint string `json:"endpoint"`
	// Exactly one of HelmRelease or Deployment is set, depending on the component type.
	HelmRelease *HelmRelease  `json:"helmRelease,omitempty"`
	Deployment  *Deployment   `json:"deployment,omitempty"`
	Gateway     *GatewayRoute `json:"gateway,omitempty"`
}

// ApplicationManifestWorkflow resolves the target cluster, then runs the manifest's steps in order.
// If the manifest has no steps, it runs one apply-component step per component in dependsOn order.
func ApplicationManifestWorkflow(ctx workflow.Context, in Input) (*Result, error) {
	r := &runner{
		in:      in,
		applied: map[string]*ComponentResult{},
		result: &Result{
			App:        in.Name,
			Namespace:  in.Namespace,
			Generation: in.Generation,
			StartedAt:  workflow.Now(ctx),
		},
	}

	steps, err := resolveSteps(in.Spec)
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidSpec", err)
	}
	progress := make([]v1alpha1.StepStatus, len(steps))
	for i, s := range steps {
		progress[i] = v1alpha1.StepStatus{Name: s.Name, Type: s.Type, Phase: "Pending"}
	}
	if err := workflow.SetQueryHandler(ctx, ProgressQuery, func() ([]v1alpha1.StepStatus, error) {
		return progress, nil
	}); err != nil {
		return nil, err
	}

	if err := workflow.ExecuteActivity(withStepOptions(ctx, ""), r.acts.ResolveTargetCluster, ResolveTargetClusterInput{
		App: in.Name, Namespace: in.Namespace, Policies: in.Spec.Policies,
	}).Get(ctx, &r.result.Cluster); err != nil {
		return nil, fmt.Errorf("resolve target cluster: %w", err)
	}

	// TODO: honor Mode=DAG and step.If; steps currently run sequentially.
	for i, step := range steps {
		progress[i].Phase = "Running"
		workflow.GetLogger(ctx).Info("Executing step", "step", step.Name, "type", step.Type)

		msg, err := r.runStep(withStepOptions(ctx, step.Timeout), step)
		if err != nil {
			progress[i].Phase = "Failed"
			progress[i].Message = err.Error()
			for j := i + 1; j < len(steps); j++ {
				progress[j].Phase = "Skipped"
			}
			return nil, fmt.Errorf("step %q failed: %w", step.Name, err)
		}
		progress[i].Phase = "Succeeded"
		progress[i].Message = msg
	}

	r.result.CompletedAt = workflow.Now(ctx)
	return r.result, nil
}

type runner struct {
	in      Input
	acts    *Activities // nil receiver; only used to reference activity methods
	applied map[string]*ComponentResult
	result  *Result
}

// runStep executes one workflow step and returns a short human-readable summary for the progress query.
func (r *runner) runStep(ctx workflow.Context, step v1alpha1.WorkflowStep) (string, error) {
	switch step.Type {
	case "apply-component":
		name := propString(step.Properties, "component", "")
		c, err := r.component(name)
		if err != nil {
			return "", err
		}
		cr, err := r.applyComponent(ctx, c)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s ready at %s", cr.Name, cr.Endpoint), nil

	case "deploy":
		ordered, err := topoSort(r.in.Spec.Components)
		if err != nil {
			return "", err
		}
		for _, c := range ordered {
			if _, err := r.applyComponent(ctx, c); err != nil {
				return "", err
			}
		}
		return fmt.Sprintf("deployed %d components to %s", len(ordered), r.result.Cluster.Name), nil

	case "notification":
		var msg SlackMessage
		if err := workflow.ExecuteActivity(ctx, r.acts.SendSlackNotification, SendSlackNotificationInput{
			Properties: step.Properties,
			Text:       r.summary(),
		}).Get(ctx, &msg); err != nil {
			return "", err
		}
		r.result.Notifications = append(r.result.Notifications, msg)
		return "posted to " + msg.Channel, nil
	}
	err := fmt.Errorf("unsupported step type %q", step.Type)
	return "", temporal.NewNonRetryableApplicationError(err.Error(), "UnsupportedStepType", err)
}

// applyComponent deploys a component once; later steps that reference it reuse the recorded result.
func (r *runner) applyComponent(ctx workflow.Context, c v1alpha1.Component) (*ComponentResult, error) {
	if cr, ok := r.applied[c.Name]; ok {
		return cr, nil
	}
	cluster := r.result.Cluster
	cr := &ComponentResult{Name: c.Name, Type: c.Type}

	switch c.Type {
	case "helm":
		var rel HelmRelease
		if err := workflow.ExecuteActivity(ctx, r.acts.InstallHelmChart, InstallHelmChartInput{
			Cluster: cluster, Namespace: r.in.Namespace, Component: c.Name, Properties: c.Properties,
		}).Get(ctx, &rel); err != nil {
			return nil, err
		}
		cr.HelmRelease, cr.Endpoint = &rel, rel.Endpoint

	case "webservice", "worker":
		deps := make(map[string]string, len(c.DependsOn))
		for _, d := range c.DependsOn {
			dep, ok := r.applied[d]
			if !ok {
				err := fmt.Errorf("component %q depends on %q, which has not been applied yet", c.Name, d)
				return nil, temporal.NewNonRetryableApplicationError(err.Error(), "DependencyNotReady", err)
			}
			deps[d] = dep.Endpoint
		}
		var dep Deployment
		if err := workflow.ExecuteActivity(ctx, r.acts.DeployWorkload, DeployWorkloadInput{
			Cluster: cluster, Namespace: r.in.Namespace, Component: c, Dependencies: deps,
		}).Get(ctx, &dep); err != nil {
			return nil, err
		}
		cr.Deployment, cr.Endpoint = &dep, dep.InternalURL

		for _, t := range c.Traits {
			if t.Type != "gateway" {
				continue // scaler is applied by DeployWorkload; other traits are not implemented yet
			}
			var route GatewayRoute
			if err := workflow.ExecuteActivity(ctx, r.acts.ConfigureGatewayRoute, ConfigureGatewayRouteInput{
				Cluster: cluster, Namespace: r.in.Namespace, ServiceName: dep.ServiceName, ServicePort: dep.Port, Properties: t.Properties,
			}).Get(ctx, &route); err != nil {
				return nil, err
			}
			cr.Gateway, cr.Endpoint = &route, route.ExternalURL
		}

	default:
		err := fmt.Errorf("component %q has unsupported type %q", c.Name, c.Type)
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), "UnsupportedComponentType", err)
	}

	r.applied[c.Name] = cr
	r.result.Components = append(r.result.Components, *cr)
	return cr, nil
}

func (r *runner) component(name string) (v1alpha1.Component, error) {
	for _, c := range r.in.Spec.Components {
		if c.Name == name {
			return c, nil
		}
	}
	err := fmt.Errorf("step references unknown component %q", name)
	return v1alpha1.Component{}, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidSpec", err)
}

func (r *runner) summary() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s/%s (gen %d) deployed to %s:", r.in.Namespace, r.in.Name, r.in.Generation, r.result.Cluster.Name)
	for _, c := range r.result.Components {
		fmt.Fprintf(&b, "\n• %s → %s", c.Name, c.Endpoint)
	}
	return b.String()
}

func withStepOptions(ctx workflow.Context, timeout string) workflow.Context {
	d := defaultStepTimeout
	if timeout != "" {
		if parsed, err := time.ParseDuration(timeout); err == nil {
			d = parsed
		}
	}
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: d,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
}

func resolveSteps(spec v1alpha1.ApplicationManifestSpec) ([]v1alpha1.WorkflowStep, error) {
	if spec.Workflow != nil && len(spec.Workflow.Steps) > 0 {
		return spec.Workflow.Steps, nil
	}
	ordered, err := topoSort(spec.Components)
	if err != nil {
		return nil, err
	}
	steps := make([]v1alpha1.WorkflowStep, 0, len(ordered))
	for _, c := range ordered {
		steps = append(steps, v1alpha1.WorkflowStep{
			Name:       "apply-" + c.Name,
			Type:       "apply-component",
			Properties: v1alpha1.Properties{"component": c.Name},
		})
	}
	return steps, nil
}

// topoSort orders components so dependencies come first, preserving spec order otherwise.
func topoSort(components []v1alpha1.Component) ([]v1alpha1.Component, error) {
	byName := make(map[string]v1alpha1.Component, len(components))
	for _, c := range components {
		byName[c.Name] = c
	}
	const (
		unvisited = iota
		visiting
		done
	)
	state := make(map[string]int, len(components))
	out := make([]v1alpha1.Component, 0, len(components))

	var visit func(name string) error
	visit = func(name string) error {
		switch state[name] {
		case done:
			return nil
		case visiting:
			return fmt.Errorf("dependency cycle at component %q", name)
		}
		c, ok := byName[name]
		if !ok {
			return fmt.Errorf("unknown component %q in dependsOn", name)
		}
		state[name] = visiting
		for _, d := range c.DependsOn {
			if err := visit(d); err != nil {
				return err
			}
		}
		state[name] = done
		out = append(out, c)
		return nil
	}
	for _, c := range components {
		if err := visit(c.Name); err != nil {
			return nil, err
		}
	}
	return out, nil
}
