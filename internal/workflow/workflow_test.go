package workflow

import (
	"encoding/json"
	"testing"

	"go.temporal.io/sdk/testsuite"

	"github.com/srekapalli/temporalcontroller/api/v1alpha1"
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
	env.RegisterActivity(&Activities{})

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
