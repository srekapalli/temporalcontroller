// Package workflow defines the Temporal workflow started for each ApplicationManifest generation.
package workflow

import (
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/srekapalli/temporalcontroller/api/v1alpha1"
)

const (
	WorkflowName  = "ApplicationManifestWorkflow"
	ProgressQuery = "progress"

	defaultStepTimeout = 10 * time.Minute
)

// Condition types. Per-component conditions are prefixed with the CamelCased component name,
// e.g. RedisHelmReleaseInstalled, CheckoutApiWorkloadDeployed, CheckoutApiGatewayRouteConfigured.
const (
	ConditionReady                  = "Ready"
	ConditionClusterResolved        = "ClusterResolved"
	ConditionNotificationSent       = "NotificationSent"
	ConditionHelmReleaseInstalled   = "HelmReleaseInstalled"
	ConditionWorkloadDeployed       = "WorkloadDeployed"
	ConditionGatewayRouteConfigured = "GatewayRouteConfigured"
)

var statusActivityOptions = workflow.ActivityOptions{
	StartToCloseTimeout: 30 * time.Second,
	RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
}

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

	r.updateStatus(ctx, v1alpha1.PhaseRunning, "",
		condition(ConditionReady, false, "Progressing", "Workflow "+workflow.GetInfo(ctx).WorkflowExecution.ID+" started"))

	steps, err := resolveSteps(in.Spec)
	if err != nil {
		return r.fail(ctx, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidSpec", err))
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
		return r.fail(ctx, r.failCondition(ConditionClusterResolved, fmt.Errorf("resolve target cluster: %w", err)))
	}
	r.updateStatus(ctx, "", "", condition(ConditionClusterResolved, true, "Resolved",
		fmt.Sprintf("Targeting %s (%s)", r.result.Cluster.Name, r.result.Cluster.Region)))

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
			return r.fail(ctx, fmt.Errorf("step %q failed: %w", step.Name, err))
		}
		progress[i].Phase = "Succeeded"
		progress[i].Message = msg
	}

	r.updateStatus(ctx, v1alpha1.PhaseSucceeded, "", condition(ConditionReady, true, "Succeeded",
		fmt.Sprintf("%d components deployed to %s", len(r.result.Components), r.result.Cluster.Name)))
	r.result.CompletedAt = workflow.Now(ctx)
	return r.result, nil
}

type runner struct {
	in      Input
	acts    *Activities // nil receiver; only used to reference activity methods
	applied map[string]*ComponentResult
	result  *Result
	// failed is the condition of the activity that failed, written together with Ready=False by fail.
	failed *metav1.Condition
}

// updateStatus writes phase and conditions to the ApplicationManifest. It is best-effort: a failed
// status write is logged and does not fail the deployment.
func (r *runner) updateStatus(ctx workflow.Context, phase v1alpha1.Phase, message string, conds ...metav1.Condition) {
	sctx := workflow.WithActivityOptions(ctx, statusActivityOptions)
	err := workflow.ExecuteActivity(sctx, r.acts.UpdateManifestStatus, UpdateManifestStatusInput{
		Namespace:  r.in.Namespace,
		Name:       r.in.Name,
		Generation: r.in.Generation,
		Phase:      phase,
		Message:    message,
		Conditions: conds,
	}).Get(sctx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Warn("Failed to update ApplicationManifest status", "err", err)
	}
}

// failCondition records condType=False for the failing activity and returns err unchanged.
func (r *runner) failCondition(condType string, err error) error {
	c := condition(condType, false, "Failed", err.Error())
	r.failed = &c
	return err
}

// fail marks the manifest Failed and returns err. Cancellation (superseded or deleted) leaves status
// to whoever cancelled the workflow.
func (r *runner) fail(ctx workflow.Context, err error) (*Result, error) {
	if temporal.IsCanceledError(err) || ctx.Err() != nil {
		return nil, err
	}
	var conds []metav1.Condition
	if r.failed != nil {
		conds = append(conds, *r.failed)
	}
	conds = append(conds, condition(ConditionReady, false, "Failed", err.Error()))
	r.updateStatus(ctx, v1alpha1.PhaseFailed, err.Error(), conds...)
	return nil, err
}

func condition(condType string, ok bool, reason, message string) metav1.Condition {
	status := metav1.ConditionFalse
	if ok {
		status = metav1.ConditionTrue
	}
	return metav1.Condition{Type: condType, Status: status, Reason: reason, Message: message}
}

// componentCondition returns e.g. "CheckoutApiWorkloadDeployed" for ("checkout-api", ConditionWorkloadDeployed).
func componentCondition(component, condType string) string {
	var b strings.Builder
	for _, part := range strings.FieldsFunc(component, func(r rune) bool { return r == '-' || r == '.' }) {
		b.WriteString(strings.ToUpper(part[:1]) + part[1:])
	}
	return b.String() + condType
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
			return "", r.failCondition(ConditionNotificationSent, err)
		}
		r.result.Notifications = append(r.result.Notifications, msg)
		r.updateStatus(ctx, "", "", condition(ConditionNotificationSent, true, "Sent",
			fmt.Sprintf("Posted to %s: %s", msg.Channel, msg.Permalink)))
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
		condType := componentCondition(c.Name, ConditionHelmReleaseInstalled)
		var rel HelmRelease
		if err := workflow.ExecuteActivity(ctx, r.acts.InstallHelmChart, InstallHelmChartInput{
			Cluster: cluster, Namespace: r.in.Namespace, Component: c.Name, Properties: c.Properties,
		}).Get(ctx, &rel); err != nil {
			return nil, r.failCondition(condType, err)
		}
		cr.HelmRelease, cr.Endpoint = &rel, rel.Endpoint
		r.updateStatus(ctx, "", "", condition(condType, true, "Installed",
			fmt.Sprintf("Release %s rev %d (%s %s) on %s, endpoint %s", rel.ReleaseName, rel.Revision, rel.Chart, rel.ChartVersion, rel.Cluster, rel.Endpoint)))

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
		condType := componentCondition(c.Name, ConditionWorkloadDeployed)
		var dep Deployment
		if err := workflow.ExecuteActivity(ctx, r.acts.DeployWorkload, DeployWorkloadInput{
			Cluster: cluster, Namespace: r.in.Namespace, Component: c, Dependencies: deps,
		}).Get(ctx, &dep); err != nil {
			return nil, r.failCondition(condType, err)
		}
		cr.Deployment, cr.Endpoint = &dep, dep.InternalURL
		r.updateStatus(ctx, "", "", condition(condType, true, "Deployed",
			fmt.Sprintf("Deployment %s %d/%d ready (%s) on %s, service %s:%d", dep.DeploymentName, dep.ReadyReplicas, dep.Replicas, dep.Image, dep.Cluster, dep.ClusterIP, dep.Port)))

		for _, t := range c.Traits {
			if t.Type != "gateway" {
				continue // scaler is applied by DeployWorkload; other traits are not implemented yet
			}
			condType := componentCondition(c.Name, ConditionGatewayRouteConfigured)
			var route GatewayRoute
			if err := workflow.ExecuteActivity(ctx, r.acts.ConfigureGatewayRoute, ConfigureGatewayRouteInput{
				Cluster: cluster, Namespace: r.in.Namespace, ServiceName: dep.ServiceName, ServicePort: dep.Port, Properties: t.Properties,
			}).Get(ctx, &route); err != nil {
				return nil, r.failCondition(condType, err)
			}
			cr.Gateway, cr.Endpoint = &route, route.ExternalURL
			r.updateStatus(ctx, "", "", condition(condType, true, "Configured",
				fmt.Sprintf("Route %s serving %s via %s", route.RouteName, route.ExternalURL, route.LoadBalancerIP)))
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
