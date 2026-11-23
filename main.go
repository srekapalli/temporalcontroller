package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"go.temporal.io/sdk/client"
	tlog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/srekapalli/temporalcontroller/internal/watcher"
	"github.com/srekapalli/temporalcontroller/internal/workflow"
)

func main() {
	var (
		kubeconfig        = flag.String("kubeconfig", "", "Path to kubeconfig. Defaults to $KUBECONFIG, ~/.kube/config, then in-cluster config.")
		watchNamespace    = flag.String("namespace", "", "Namespace to watch. Empty watches all namespaces.")
		temporalAddress   = flag.String("temporal-address", "localhost:7233", "Temporal frontend host:port.")
		temporalNamespace = flag.String("temporal-namespace", "default", "Temporal namespace.")
		taskQueue         = flag.String("task-queue", "applicationmanifest", "Temporal task queue for workflows.")
		workers           = flag.Int("workers", 2, "Number of concurrent reconcile workers.")
		runWorker         = flag.Bool("run-worker", true, "Also run a Temporal worker for the workflow in this process.")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(log)
	if err := run(log, *kubeconfig, *watchNamespace, *temporalAddress, *temporalNamespace, *taskQueue, *workers, *runWorker); err != nil {
		log.Error("Exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, kubeconfig, watchNamespace, temporalAddress, temporalNamespace, taskQueue string, workers int, runWorker bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = kubeconfig
	restCfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return err
	}
	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return err
	}

	tc, err := client.Dial(client.Options{
		HostPort:  temporalAddress,
		Namespace: temporalNamespace,
		Logger:    tlog.NewStructuredLogger(log),
	})
	if err != nil {
		return err
	}
	defer tc.Close()

	if runWorker {
		w := worker.New(tc, taskQueue, worker.Options{})
		w.RegisterWorkflowWithOptions(workflow.ApplicationManifestWorkflow, sdkworkflow.RegisterOptions{Name: workflow.WorkflowName})
		w.RegisterActivity(&workflow.Activities{K8s: dyn})
		if err := w.Start(); err != nil {
			return err
		}
		defer w.Stop()
	}

	wt, err := watcher.New(watcher.Config{Namespace: watchNamespace, TaskQueue: taskQueue, Workers: workers}, dyn, tc, log)
	if err != nil {
		return err
	}
	return wt.Run(ctx)
}
