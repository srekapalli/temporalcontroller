package workflow

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/srekapalli/temporalcontroller/api/v1alpha1"
)

// newFakeK8s returns a fake dynamic client holding one ApplicationManifest shop/checkout at the given generation.
func newFakeK8s(generation int64) dynamic.Interface {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(v1alpha1.Group + "/" + v1alpha1.Version)
	u.SetKind(v1alpha1.Kind)
	u.SetNamespace("shop")
	u.SetName("checkout")
	u.SetGeneration(generation)
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{v1alpha1.GroupVersionResource: v1alpha1.Kind + "List"}, u)
}

func getStatus(t *testing.T, k8s dynamic.Interface) v1alpha1.ApplicationManifestStatus {
	t.Helper()
	u, err := k8s.Resource(v1alpha1.GroupVersionResource).Namespace("shop").Get(context.Background(), "checkout", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var am v1alpha1.ApplicationManifest
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &am); err != nil {
		t.Fatal(err)
	}
	return am.Status
}

func findCondition(conds []metav1.Condition, condType string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == condType {
			return &conds[i]
		}
	}
	return nil
}

var (
	mockAny      = mock.Anything
	errImagePull = temporal.NewNonRetryableApplicationError("ErrImagePull: ghcr.io/acme/checkout:1.4.2", "ImagePull", nil)
)

// checkoutSpec mirrors examples/checkout.yaml.
func checkoutSpec() v1alpha1.ApplicationManifestSpec {
	return v1alpha1.ApplicationManifestSpec{
		Components: []v1alpha1.Component{
			{Name: "redis", Type: "helm", Properties: v1alpha1.Properties{"chart": "bitnami/redis", "version": "19.0.1"}},
			{
				Name: "checkout-api", Type: "webservice", DependsOn: []string{"redis"},
				Properties: v1alpha1.Properties{
					"image": "ghcr.io/acme/checkout:1.4.2",
					"port":  8080,
					"env":   []any{map[string]any{"name": "LOG_LEVEL", "value": "info"}},
				},
				Traits: []v1alpha1.Trait{
					{Type: "scaler", Properties: v1alpha1.Properties{"replicas": 3}},
					{Type: "gateway", Properties: v1alpha1.Properties{"host": "checkout.acme.dev", "path": "/"}},
				},
			},
		},
		Policies: []v1alpha1.Policy{
			{Name: "prod-only", Type: "topology", Properties: v1alpha1.Properties{"clusters": []any{"prod-us-east"}}},
		},
		Workflow: &v1alpha1.Workflow{Steps: []v1alpha1.WorkflowStep{
			{Name: "deploy-redis", Type: "apply-component", Properties: v1alpha1.Properties{"component": "redis"}, Timeout: "5m"},
			{Name: "deploy-api", Type: "apply-component", Properties: v1alpha1.Properties{"component": "checkout-api"}},
			{Name: "notify", Type: "notification", Properties: v1alpha1.Properties{"slack": map[string]any{"channel": "#deploys"}}},
		}},
	}
}

func TestCheckoutWorkflowResult(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	k8s := newFakeK8s(3)
	env.RegisterActivity(&Activities{K8s: k8s})

	env.ExecuteWorkflow(ApplicationManifestWorkflow, Input{Namespace: "shop", Name: "checkout", Generation: 3, Spec: checkoutSpec()})
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("workflow did not succeed: %v", env.GetWorkflowError())
	}

	var res Result
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	pretty, _ := json.MarshalIndent(res, "", "  ")
	t.Logf("workflow result:\n%s", pretty)

	if res.Cluster.Name != "prod-us-east" {
		t.Errorf("cluster = %q, want prod-us-east", res.Cluster.Name)
	}
	if len(res.Components) != 2 {
		t.Fatalf("got %d components, want 2", len(res.Components))
	}
	redis, api := res.Components[0], res.Components[1]
	if want := "redis://redis-master.shop.svc.cluster.local:6379"; redis.Endpoint != want {
		t.Errorf("redis endpoint = %q, want %q", redis.Endpoint, want)
	}
	if api.Deployment == nil || api.Deployment.DeploymentName != "checkout-api" || api.Deployment.Replicas != 3 {
		t.Errorf("unexpected deployment: %+v", api.Deployment)
	}
	if got := api.Deployment.Env["REDIS_URL"]; got != redis.Endpoint {
		t.Errorf("REDIS_URL = %q, want redis endpoint injected", got)
	}
	if api.Endpoint != "https://checkout.acme.dev" {
		t.Errorf("api endpoint = %q, want gateway URL", api.Endpoint)
	}
	if len(res.Notifications) != 1 || res.Notifications[0].Channel != "#deploys" {
		t.Errorf("unexpected notifications: %+v", res.Notifications)
	}

	q, err := env.QueryWorkflow(ProgressQuery)
	if err != nil {
		t.Fatal(err)
	}
	var progress []v1alpha1.StepStatus
	if err := q.Get(&progress); err != nil {
		t.Fatal(err)
	}
	for _, p := range progress {
		if p.Phase != "Succeeded" {
			t.Errorf("step %s phase = %s, want Succeeded", p.Name, p.Phase)
		}
		t.Logf("step %-13s %-9s %s", p.Name, p.Phase, p.Message)
	}

	status := getStatus(t, k8s)
	if status.Phase != v1alpha1.PhaseSucceeded {
		t.Errorf("phase = %q, want Succeeded", status.Phase)
	}
	wantConds := []string{
		ConditionReady,
		ConditionClusterResolved,
		"RedisHelmReleaseInstalled",
		"CheckoutApiWorkloadDeployed",
		"CheckoutApiGatewayRouteConfigured",
		ConditionNotificationSent,
	}
	for _, ct := range wantConds {
		c := findCondition(status.Conditions, ct)
		if c == nil || c.Status != metav1.ConditionTrue || c.ObservedGeneration != 3 {
			t.Errorf("condition %s = %+v, want True at generation 3", ct, c)
		}
	}
	for _, c := range status.Conditions {
		t.Logf("condition %-34s %-5s %-11s %s", c.Type, c.Status, c.Reason, c.Message)
	}
}

func TestFailedActivitySetsConditions(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	k8s := newFakeK8s(1)
	acts := &Activities{K8s: k8s}
	env.RegisterActivity(acts)
	env.OnActivity(acts.DeployWorkload, mockAny, mockAny).Return(Deployment{}, errImagePull)

	env.ExecuteWorkflow(ApplicationManifestWorkflow, Input{Namespace: "shop", Name: "checkout", Generation: 1, Spec: checkoutSpec()})
	if env.GetWorkflowError() == nil {
		t.Fatal("expected workflow to fail")
	}

	status := getStatus(t, k8s)
	if status.Phase != v1alpha1.PhaseFailed {
		t.Errorf("phase = %q, want Failed", status.Phase)
	}
	if c := findCondition(status.Conditions, "RedisHelmReleaseInstalled"); c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("redis condition = %+v, want True", c)
	}
	if c := findCondition(status.Conditions, "CheckoutApiWorkloadDeployed"); c == nil || c.Status != metav1.ConditionFalse || c.Reason != "Failed" {
		t.Errorf("checkout-api condition = %+v, want False/Failed", c)
	}
	if c := findCondition(status.Conditions, ConditionReady); c == nil || c.Status != metav1.ConditionFalse || c.Reason != "Failed" {
		t.Errorf("Ready = %+v, want False/Failed", c)
	}
	if c := findCondition(status.Conditions, "CheckoutApiGatewayRouteConfigured"); c != nil {
		t.Errorf("gateway condition should not be set after deploy failure, got %+v", c)
	}
}

func TestUpdateManifestStatusIgnoresStaleGeneration(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestActivityEnvironment()
	k8s := newFakeK8s(5)
	acts := &Activities{K8s: k8s}
	env.RegisterActivity(acts)

	_, err := env.ExecuteActivity(acts.UpdateManifestStatus, UpdateManifestStatusInput{
		Namespace: "shop", Name: "checkout", Generation: 4, Phase: v1alpha1.PhaseRunning,
		Conditions: []metav1.Condition{condition(ConditionReady, false, "Progressing", "old workflow")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if status := getStatus(t, k8s); status.Phase != "" || len(status.Conditions) != 0 {
		t.Errorf("stale write was applied: %+v", status)
	}
}

func TestDefaultStepsFollowDependsOn(t *testing.T) {
	steps, err := resolveSteps(v1alpha1.ApplicationManifestSpec{Components: []v1alpha1.Component{
		{Name: "api", Type: "webservice", DependsOn: []string{"redis"}},
		{Name: "redis", Type: "helm"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 2 || steps[0].Name != "apply-redis" || steps[1].Name != "apply-api" {
		t.Fatalf("unexpected steps: %+v", steps)
	}
}

func TestDependencyCycleFails(t *testing.T) {
	_, err := topoSort([]v1alpha1.Component{
		{Name: "a", DependsOn: []string{"b"}},
		{Name: "b", DependsOn: []string{"a"}},
	})
	if err == nil {
		t.Fatal("expected cycle error")
	}
}
