package main

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const defaultSchedulerName = "dumb-scheduler"

func main() {
	var (
		kubeconfig    string
		schedulerName string
	)
	if home := homedir.HomeDir(); home != "" {
		flag.StringVar(&kubeconfig, "kubeconfig", filepath.Join(home, ".kube", "config"),
			"path to kubeconfig (only used when running outside the cluster)")
	} else {
		flag.StringVar(&kubeconfig, "kubeconfig", "", "path to kubeconfig")
	}
	flag.StringVar(&schedulerName, "scheduler-name", defaultSchedulerName,
		"only schedule pods whose spec.schedulerName matches this value")
	klog.InitFlags(nil)
	flag.Parse()

	cfg, err := buildConfig(kubeconfig)
	if err != nil {
		klog.Fatalf("building client config: %v", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("building clientset: %v", err)
	}

	ctx := signalContext()
	s := newScheduler(client, schedulerName)
	if err := s.run(ctx); err != nil {
		klog.Fatalf("scheduler exited: %v", err)
	}
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	return clientcmd.BuildConfigFromFlags("", kubeconfig)
}

type scheduler struct {
	client        kubernetes.Interface
	schedulerName string

	factory     informers.SharedInformerFactory
	nodeFactory informers.SharedInformerFactory
	podsView    cache.SharedIndexInformer
	nodeView    cache.SharedIndexInformer
	queue       workqueue.RateLimitingInterface
}

func newScheduler(client kubernetes.Interface, schedulerName string) *scheduler {
	factory := informers.NewSharedInformerFactoryWithOptions(client, 0,
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.FieldSelector = fields.OneTermEqualSelector(
				"spec.schedulerName", schedulerName).String()
		}),
	)
	nodeFactory := informers.NewSharedInformerFactory(client, 0)

	s := &scheduler{
		client:        client,
		schedulerName: schedulerName,
		factory:       factory,
		nodeFactory:   nodeFactory,
		podsView:      factory.Core().V1().Pods().Informer(),
		nodeView:      nodeFactory.Core().V1().Nodes().Informer(),
		queue:         workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "scheduler"),
	}

	s.podsView.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: s.enqueuePodIfPending,
		UpdateFunc: func(_, newObj interface{}) {
			s.enqueuePodIfPending(newObj)
		},
	})
	return s
}

func (s *scheduler) run(ctx context.Context) error {
	defer s.queue.ShutDown()

	klog.Infof("starting %q scheduler (one pod per node, no resource accounting)", s.schedulerName)

	s.factory.Start(ctx.Done())
	s.nodeFactory.Start(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), s.podsView.HasSynced, s.nodeView.HasSynced) {
		return fmt.Errorf("failed to sync informer caches")
	}
	klog.Info("informer caches synced; ready to schedule")

	go wait.UntilWithContext(ctx, s.worker, time.Second)

	<-ctx.Done()
	klog.Info("shutting down")
	return nil
}
