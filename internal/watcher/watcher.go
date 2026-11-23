// Package watcher watches ApplicationManifest objects and starts a Temporal workflow for every new spec generation.
package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/srekapalli/temporalcontroller/api/v1alpha1"
	"github.com/srekapalli/temporalcontroller/internal/workflow"
)

type Config struct {
	Namespace string // empty watches all namespaces
	TaskQueue string
	Workers   int
}

type Watcher struct {
	cfg      Config
	dyn      dynamic.Interface
	temporal client.Client
	informer cache.SharedIndexInformer
	queue    workqueue.TypedRateLimitingInterface[string]
	log      *slog.Logger
}

func New(cfg Config, dyn dynamic.Interface, tc client.Client, log *slog.Logger) (*Watcher, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = 2
	}
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dyn, 0, cfg.Namespace, nil)
	w := &Watcher{
		cfg:      cfg,
		dyn:      dyn,
		temporal: tc,
		informer: factory.ForResource(v1alpha1.GroupVersionResource).Informer(),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "applicationmanifests"},
		),
		log: log,
	}

	_, err := w.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: w.enqueue,
		UpdateFunc: func(oldObj, newObj any) {
			// generation only changes on spec updates, so status patches and metadata edits are ignored.
			if oldObj.(*unstructured.Unstructured).GetGeneration() != newObj.(*unstructured.Unstructured).GetGeneration() {
				w.enqueue(newObj)
			}
		},
		DeleteFunc: w.onDelete,
	})
	if err != nil {
		return nil, err
	}
	return w, nil
}

// Run blocks until ctx is cancelled.
func (w *Watcher) Run(ctx context.Context) error {
	defer w.queue.ShutDown()

	go w.informer.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), w.informer.HasSynced) {
		return errors.New("timed out waiting for informer cache to sync")
	}
	w.log.Info("Watching ApplicationManifests", "namespace", w.cfg.Namespace, "workers", w.cfg.Workers)

	var wg sync.WaitGroup
	for range w.cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for w.processNext(ctx) {
			}
		}()
	}
	<-ctx.Done()
	w.queue.ShutDown()
	wg.Wait()
	return nil
}

func (w *Watcher) enqueue(obj any) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		w.log.Error("Failed to compute key", "err", err)
		return
	}
	w.queue.Add(key)
}

func (w *Watcher) processNext(ctx context.Context) bool {
	key, shutdown := w.queue.Get()
	if shutdown {
		return false
	}
	defer w.queue.Done(key)

	if err := w.reconcile(ctx, key); err != nil {
		w.log.Error("Reconcile failed, requeueing", "key", key, "err", err)
		w.queue.AddRateLimited(key)
		return true
	}
	w.queue.Forget(key)
	return true
}

func (w *Watcher) reconcile(ctx context.Context, key string) error {
	obj, exists, err := w.informer.GetIndexer().GetByKey(key)
	if err != nil || !exists {
		return err
	}
	am, err := fromUnstructured(obj.(*unstructured.Unstructured))
	if err != nil {
		return err
	}
	log := w.log.With("key", key, "generation", am.Generation)

	prev := am.Status.Workflow
	if prev != nil && prev.ObservedGeneration >= am.Generation {
		return nil
	}
	if am.Spec.Suspend {
		if am.Status.Phase == v1alpha1.PhaseSuspended {
			return nil
		}
		log.Info("Suspended, not starting workflow")
		return w.patchStatus(ctx, am, map[string]any{"phase": v1alpha1.PhaseSuspended})
	}

	// One workflow ID per (object, generation): starting is idempotent if we crash before the status patch lands.
	wfID := fmt.Sprintf("appm-%s-g%d", am.UID, am.Generation)
	run, err := w.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       wfID,
		TaskQueue:                w.cfg.TaskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		Memo:                     map[string]any{"namespace": am.Namespace, "name": am.Name},
	}, workflow.WorkflowName, workflow.Input{
		Namespace:  am.Namespace,
		Name:       am.Name,
		UID:        string(am.UID),
		Generation: am.Generation,
		Spec:       am.Spec,
	})
	if err != nil {
		return fmt.Errorf("start workflow %s: %w", wfID, err)
	}
	log.Info("Started workflow", "workflowID", run.GetID(), "runID", run.GetRunID())

	// The newest spec wins: cancel the run for the generation we last dispatched.
	if prev != nil && prev.WorkflowID != "" && prev.WorkflowID != wfID {
		w.cancel(ctx, prev.WorkflowID, "superseded by generation", am.Generation)
	}

	return w.patchStatus(ctx, am, map[string]any{
		"phase":   nil,
		"message": nil,
		"workflow": map[string]any{
			"observedGeneration": am.Generation,
			"workflowID":         run.GetID(),
			"runID":              run.GetRunID(),
			"taskQueue":          w.cfg.TaskQueue,
			"startTime":          metav1.Now().UTC().Format(time.RFC3339),
			// Execution status is not tracked yet; clear fields left over from the previous generation.
			"status":    nil,
			"closeTime": nil,
			"message":   nil,
			"steps":     nil,
		},
	})
}

func (w *Watcher) onDelete(obj any) {
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	am, err := fromUnstructured(u)
	if err != nil || am.Status.Workflow == nil || am.Status.Workflow.WorkflowID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w.cancel(ctx, am.Status.Workflow.WorkflowID, "ApplicationManifest deleted", am.Generation)
}

func (w *Watcher) cancel(ctx context.Context, workflowID, reason string, generation int64) {
	err := w.temporal.CancelWorkflow(ctx, workflowID, "")
	var notFound *serviceerror.NotFound
	switch {
	case err == nil:
		w.log.Info("Cancelled workflow", "workflowID", workflowID, "reason", reason, "generation", generation)
	case errors.As(err, &notFound):
		// Already completed or never existed.
	default:
		w.log.Warn("Failed to cancel workflow", "workflowID", workflowID, "err", err)
	}
}

func (w *Watcher) patchStatus(ctx context.Context, am *v1alpha1.ApplicationManifest, status map[string]any) error {
	body, err := json.Marshal(map[string]any{"status": status})
	if err != nil {
		return err
	}
	_, err = w.dyn.Resource(v1alpha1.GroupVersionResource).Namespace(am.Namespace).
		Patch(ctx, am.Name, types.MergePatchType, body, metav1.PatchOptions{}, "status")
	return err
}

func fromUnstructured(u *unstructured.Unstructured) (*v1alpha1.ApplicationManifest, error) {
	var am v1alpha1.ApplicationManifest
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &am); err != nil {
		return nil, fmt.Errorf("convert %s/%s: %w", u.GetNamespace(), u.GetName(), err)
	}
	return &am, nil
}
