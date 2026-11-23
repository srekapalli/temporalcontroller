// Package v1alpha1 holds hand-written Go types for the ApplicationManifest CRD.
//
// These types are not registered in a scheme and have no generated deepcopy.
// The watcher reads objects as unstructured and converts them with
// runtime.DefaultUnstructuredConverter, so plain structs are enough.
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	Group    = "apps.temporalcontroller.io"
	Version  = "v1alpha1"
	Kind     = "ApplicationManifest"
	Resource = "applicationmanifests"
)

var GroupVersionResource = schema.GroupVersionResource{Group: Group, Version: Version, Resource: Resource}

type ApplicationManifest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ApplicationManifestSpec   `json:"spec"`
	Status ApplicationManifestStatus `json:"status,omitempty"`
}

type ApplicationManifestSpec struct {
	Suspend    bool        `json:"suspend,omitempty"`
	Components []Component `json:"components"`
	Policies   []Policy    `json:"policies,omitempty"`
	Workflow   *Workflow   `json:"workflow,omitempty"`
}

// Properties is free-form, type-specific configuration (x-kubernetes-preserve-unknown-fields).
type Properties map[string]any

type Component struct {
	Name       string     `json:"name"`
	Type       string     `json:"type"`
	Properties Properties `json:"properties,omitempty"`
	DependsOn  []string   `json:"dependsOn,omitempty"`
	Traits     []Trait    `json:"traits,omitempty"`
}

type Trait struct {
	Type       string     `json:"type"`
	Properties Properties `json:"properties,omitempty"`
}

type Policy struct {
	Name       string     `json:"name"`
	Type       string     `json:"type"`
	Properties Properties `json:"properties,omitempty"`
}

type WorkflowMode string

const (
	WorkflowModeStepByStep WorkflowMode = "StepByStep"
	WorkflowModeDAG        WorkflowMode = "DAG"
)

type Workflow struct {
	Mode  WorkflowMode   `json:"mode,omitempty"`
	Steps []WorkflowStep `json:"steps,omitempty"`
}

type WorkflowStep struct {
	Name       string     `json:"name"`
	Type       string     `json:"type"`
	Properties Properties `json:"properties,omitempty"`
	DependsOn  []string   `json:"dependsOn,omitempty"`
	If         string     `json:"if,omitempty"`
	Timeout    string     `json:"timeout,omitempty"`
}

type Phase string

const (
	PhasePending   Phase = "Pending"
	PhaseRunning   Phase = "Running"
	PhaseSucceeded Phase = "Succeeded"
	PhaseFailed    Phase = "Failed"
	PhaseSuspended Phase = "Suspended"
)

type ApplicationManifestStatus struct {
	Phase      Phase              `json:"phase,omitempty"`
	Message    string             `json:"message,omitempty"`
	Workflow   *WorkflowStatus    `json:"workflow,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

type WorkflowStatus struct {
	ObservedGeneration int64        `json:"observedGeneration,omitempty"`
	WorkflowID         string       `json:"workflowID,omitempty"`
	RunID              string       `json:"runID,omitempty"`
	TaskQueue          string       `json:"taskQueue,omitempty"`
	Status             string       `json:"status,omitempty"`
	StartTime          *metav1.Time `json:"startTime,omitempty"`
	CloseTime          *metav1.Time `json:"closeTime,omitempty"`
	Message            string       `json:"message,omitempty"`
	Steps              []StepStatus `json:"steps,omitempty"`
}

type StepStatus struct {
	Name      string       `json:"name"`
	Type      string       `json:"type,omitempty"`
	Phase     string       `json:"phase,omitempty"`
	Message   string       `json:"message,omitempty"`
	StartTime *metav1.Time `json:"startTime,omitempty"`
	EndTime   *metav1.Time `json:"endTime,omitempty"`
}
