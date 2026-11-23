package workflow

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/retry"

	"github.com/srekapalli/temporalcontroller/api/v1alpha1"
)

// Activities are stubs: they log what they would do and return made-up but plausible results.
// The exception is UpdateManifestStatus, which writes to the ApplicationManifest's status.
// Register with worker.RegisterActivity(&Activities{...}); each method is registered under its own name.
type Activities struct {
	// K8s is used by UpdateManifestStatus. If nil, status updates are logged and skipped.
	K8s dynamic.Interface
}

// UpdateManifestStatus

type UpdateManifestStatusInput struct {
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
	Generation int64  `json:"generation"`
	// Phase and Message are written only when Phase is set.
	Phase      v1alpha1.Phase     `json:"phase,omitempty"`
	Message    string             `json:"message,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// UpdateManifestStatus merges conditions into the ApplicationManifest's status.
// Writes from a workflow whose generation no longer matches the object are ignored, and conditions left
// from older generations are dropped, so a superseded workflow can't overwrite a newer one's status.
func (a *Activities) UpdateManifestStatus(ctx context.Context, in UpdateManifestStatusInput) error {
	logger := activity.GetLogger(ctx)
	if a.K8s == nil {
		logger.Info("No Kubernetes client, skipping status update", "phase", in.Phase, "conditions", len(in.Conditions))
		return nil
	}
	res := a.K8s.Resource(v1alpha1.GroupVersionResource).Namespace(in.Namespace)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		u, err := res.Get(ctx, in.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			logger.Info("ApplicationManifest is gone, skipping status update", "name", in.Name)
			return nil
		}
		if err != nil {
			return err
		}
		if u.GetGeneration() != in.Generation {
			logger.Info("Stale generation, skipping status update", "workflowGeneration", in.Generation, "objectGeneration", u.GetGeneration())
			return nil
		}

		var am v1alpha1.ApplicationManifest
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &am); err != nil {
			return err
		}
		conds := make([]metav1.Condition, 0, len(am.Status.Conditions)+len(in.Conditions))
		for _, c := range am.Status.Conditions {
			if c.ObservedGeneration >= in.Generation {
				conds = append(conds, c)
			}
		}
		for _, c := range in.Conditions {
			c.ObservedGeneration = in.Generation
			meta.SetStatusCondition(&conds, c)
		}
		am.Status.Conditions = conds
		if in.Phase != "" {
			am.Status.Phase = in.Phase
			am.Status.Message = in.Message
		}

		status, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&am.Status)
		if err != nil {
			return err
		}
		u.Object["status"] = status
		if _, err := res.UpdateStatus(ctx, u, metav1.UpdateOptions{}); err != nil {
			return err
		}
		for _, c := range in.Conditions {
			logger.Info("Set condition", "type", c.Type, "status", c.Status, "reason", c.Reason)
		}
		return nil
	})
}

// ResolveTargetCluster

type ResolveTargetClusterInput struct {
	App       string            `json:"app"`
	Namespace string            `json:"namespace"`
	Policies  []v1alpha1.Policy `json:"policies,omitempty"`
}

type TargetCluster struct {
	Name      string `json:"name"`
	Region    string `json:"region"`
	APIServer string `json:"apiServer"`
}

// ResolveTargetCluster picks the cluster to deploy to from the first topology policy, defaulting to kind-local.
func (a *Activities) ResolveTargetCluster(ctx context.Context, in ResolveTargetClusterInput) (TargetCluster, error) {
	name := "kind-local"
	for _, p := range in.Policies {
		if p.Type != "topology" {
			continue
		}
		if clusters, ok := p.Properties["clusters"].([]any); ok && len(clusters) > 0 {
			if s, ok := clusters[0].(string); ok {
				name = s
			}
		}
		break
	}
	region := "local"
	if strings.HasPrefix(name, "prod-") {
		region = strings.TrimPrefix(name, "prod-") + "-1"
	}
	out := TargetCluster{
		Name:      name,
		Region:    region,
		APIServer: fmt.Sprintf("https://%s.k8s.acme.internal:6443", name),
	}
	activity.GetLogger(ctx).Info("Resolved target cluster", "app", in.App, "cluster", out.Name, "region", out.Region)
	return out, simulateWork(ctx)
}

// InstallHelmChart

type InstallHelmChartInput struct {
	Cluster    TargetCluster       `json:"cluster"`
	Namespace  string              `json:"namespace"`
	Component  string              `json:"component"`
	Properties v1alpha1.Properties `json:"properties,omitempty"`
}

type HelmRelease struct {
	ReleaseName  string `json:"releaseName"`
	Chart        string `json:"chart"`
	ChartVersion string `json:"chartVersion"`
	Namespace    string `json:"namespace"`
	Cluster      string `json:"cluster"`
	Revision     int    `json:"revision"`
	Status       string `json:"status"`
	ServiceName  string `json:"serviceName"`
	Endpoint     string `json:"endpoint"`
}

func (a *Activities) InstallHelmChart(ctx context.Context, in InstallHelmChartInput) (HelmRelease, error) {
	chart := propString(in.Properties, "chart", in.Component)
	out := HelmRelease{
		ReleaseName:  in.Component,
		Chart:        chart,
		ChartVersion: propString(in.Properties, "version", "latest"),
		Namespace:    in.Namespace,
		Cluster:      in.Cluster.Name,
		Revision:     1,
		Status:       "deployed",
		ServiceName:  in.Component,
	}
	host := func(svc string) string { return fmt.Sprintf("%s.%s.svc.cluster.local", svc, in.Namespace) }
	switch {
	case strings.Contains(chart, "redis"):
		out.ServiceName = in.Component + "-master"
		out.Endpoint = fmt.Sprintf("redis://%s:6379", host(out.ServiceName))
	case strings.Contains(chart, "postgres"):
		out.ServiceName = in.Component + "-postgresql"
		out.Endpoint = fmt.Sprintf("postgres://app@%s:5432/app", host(out.ServiceName))
	default:
		out.Endpoint = fmt.Sprintf("http://%s:80", host(out.ServiceName))
	}
	activity.GetLogger(ctx).Info("Installed Helm chart", "release", out.ReleaseName, "chart", out.Chart, "version", out.ChartVersion, "cluster", out.Cluster)
	return out, simulateWork(ctx)
}

// DeployWorkload

type DeployWorkloadInput struct {
	Cluster   TargetCluster      `json:"cluster"`
	Namespace string             `json:"namespace"`
	Component v1alpha1.Component `json:"component"`
	// Dependencies maps each dependsOn component name to its endpoint; each one is injected as <NAME>_URL.
	Dependencies map[string]string `json:"dependencies,omitempty"`
}

type Deployment struct {
	DeploymentName string            `json:"deploymentName"`
	Namespace      string            `json:"namespace"`
	Cluster        string            `json:"cluster"`
	Image          string            `json:"image"`
	Replicas       int               `json:"replicas"`
	ReadyReplicas  int               `json:"readyReplicas"`
	ServiceName    string            `json:"serviceName"`
	ClusterIP      string            `json:"clusterIP"`
	Port           int               `json:"port"`
	InternalURL    string            `json:"internalURL"`
	Env            map[string]string `json:"env,omitempty"`
}

// DeployWorkload creates the Deployment and Service for a webservice or worker component.
// Replicas come from the component's scaler trait, if present.
func (a *Activities) DeployWorkload(ctx context.Context, in DeployWorkloadInput) (Deployment, error) {
	c := in.Component
	port := propInt(c.Properties, "port", 8080)
	replicas := 1
	for _, t := range c.Traits {
		if t.Type == "scaler" {
			replicas = propInt(t.Properties, "replicas", replicas)
		}
	}

	env := map[string]string{}
	if list, ok := c.Properties["env"].([]any); ok {
		for _, e := range list {
			if m, ok := e.(map[string]any); ok {
				name, _ := m["name"].(string)
				value, _ := m["value"].(string)
				if name != "" {
					env[name] = value
				}
			}
		}
	}
	for dep, endpoint := range in.Dependencies {
		env[strings.ToUpper(strings.ReplaceAll(dep, "-", "_"))+"_URL"] = endpoint
	}

	out := Deployment{
		DeploymentName: c.Name,
		Namespace:      in.Namespace,
		Cluster:        in.Cluster.Name,
		Image:          propString(c.Properties, "image", "nginx:stable"),
		Replicas:       replicas,
		ReadyReplicas:  replicas,
		ServiceName:    c.Name,
		ClusterIP:      fakeIP("10.96", in.Namespace+"/"+c.Name),
		Port:           port,
		InternalURL:    fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", c.Name, in.Namespace, port),
		Env:            env,
	}
	activity.GetLogger(ctx).Info("Deployed workload", "deployment", out.DeploymentName, "image", out.Image, "replicas", out.Replicas, "cluster", out.Cluster)
	return out, simulateWork(ctx)
}

// ConfigureGatewayRoute

type ConfigureGatewayRouteInput struct {
	Cluster     TargetCluster       `json:"cluster"`
	Namespace   string              `json:"namespace"`
	ServiceName string              `json:"serviceName"`
	ServicePort int                 `json:"servicePort"`
	Properties  v1alpha1.Properties `json:"properties,omitempty"`
}

type GatewayRoute struct {
	RouteName      string `json:"routeName"`
	Host           string `json:"host"`
	Path           string `json:"path"`
	ExternalURL    string `json:"externalURL"`
	LoadBalancerIP string `json:"loadBalancerIP"`
	TLS            bool   `json:"tls"`
}

// ConfigureGatewayRoute exposes a service through the gateway trait's host and path.
func (a *Activities) ConfigureGatewayRoute(ctx context.Context, in ConfigureGatewayRouteInput) (GatewayRoute, error) {
	host := propString(in.Properties, "host", fmt.Sprintf("%s.%s.apps.acme.dev", in.ServiceName, in.Namespace))
	path := propString(in.Properties, "path", "/")
	out := GatewayRoute{
		RouteName:      in.ServiceName + "-route",
		Host:           host,
		Path:           path,
		ExternalURL:    "https://" + host + strings.TrimSuffix(path, "/"),
		LoadBalancerIP: fakeIP("34.120", in.Cluster.Name),
		TLS:            true,
	}
	activity.GetLogger(ctx).Info("Configured gateway route", "route", out.RouteName, "url", out.ExternalURL)
	return out, simulateWork(ctx)
}

// SendSlackNotification

type SendSlackNotificationInput struct {
	Properties v1alpha1.Properties `json:"properties,omitempty"`
	Text       string              `json:"text"`
}

type SlackMessage struct {
	Channel   string `json:"channel"`
	MessageTS string `json:"messageTS"`
	Permalink string `json:"permalink"`
	Text      string `json:"text"`
}

func (a *Activities) SendSlackNotification(ctx context.Context, in SendSlackNotificationInput) (SlackMessage, error) {
	channel := "#deploys"
	if slack, ok := in.Properties["slack"].(map[string]any); ok {
		if ch, ok := slack["channel"].(string); ok && ch != "" {
			channel = ch
		}
	}
	ts := fmt.Sprintf("%d.000100", activity.GetInfo(ctx).StartedTime.Unix())
	out := SlackMessage{
		Channel:   channel,
		MessageTS: ts,
		Permalink: fmt.Sprintf("https://acme.slack.com/archives/%s/p%s", strings.TrimPrefix(channel, "#"), strings.ReplaceAll(ts, ".", "")),
		Text:      in.Text,
	}
	activity.GetLogger(ctx).Info("Sent Slack notification", "channel", out.Channel, "text", out.Text)
	return out, simulateWork(ctx)
}

// helpers

func simulateWork(ctx context.Context) error {
	select {
	case <-time.After(300 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func propString(p v1alpha1.Properties, key, def string) string {
	if s, ok := p[key].(string); ok && s != "" {
		return s
	}
	return def
}

// propInt accepts the numeric types produced by both the unstructured converter and JSON decoding.
func propInt(p v1alpha1.Properties, key string, def int) int {
	switch v := p[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return def
}

// fakeIP returns a stable made-up address under prefix (e.g. "10.96") for the given seed.
func fakeIP(prefix, seed string) string {
	h := fnv.New32a()
	h.Write([]byte(seed))
	sum := h.Sum32()
	return fmt.Sprintf("%s.%d.%d", prefix, (sum>>8)%254+1, sum%254+1)
}
