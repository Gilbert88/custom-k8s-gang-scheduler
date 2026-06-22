package main

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)

func TestPickFreeNode_BasicOnePerNode(t *testing.T) {
	tests := []struct {
		name       string
		nodes      []*corev1.Node
		pods       []*corev1.Pod
		shouldFail bool
	}{
		{
			name: "all nodes free - picks first free",
			nodes: []*corev1.Node{
				makeNode("node1", true),
				makeNode("node2", true),
			},
			pods:       []*corev1.Pod{},
			shouldFail: false,
		},
		{
			name: "some nodes occupied - picks free one",
			nodes: []*corev1.Node{
				makeNode("node1", true),
				makeNode("node2", true),
			},
			pods: []*corev1.Pod{
				makePod("default", "pod1", "node1", nil),
			},
			shouldFail: false,
		},
		{
			name: "all nodes occupied - fails without preemption",
			nodes: []*corev1.Node{
				makeNode("node1", true),
				makeNode("node2", true),
			},
			pods: []*corev1.Pod{
				makePod("default", "pod1", "node1", nil),
				makePod("default", "pod2", "node2", nil),
			},
			shouldFail: true,
		},
		{
			name: "unschedulable node skipped",
			nodes: []*corev1.Node{
				makeNode("node1", false), // unschedulable
				makeNode("node2", true),
			},
			pods:       []*corev1.Pod{},
			shouldFail: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := makeScheduler(tt.nodes, tt.pods)

			pod := makePod("default", "test-pod", "", nil)
			node, err := s.pickFreeNode(pod)

			if tt.shouldFail && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.shouldFail && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tt.shouldFail && node == "" {
				t.Errorf("expected node assignment, got empty")
			}
		})
	}
}

func TestPickFreeNode_Preemption(t *testing.T) {
	tests := []struct {
		name          string
		nodes         []*corev1.Node
		existingPods  []*corev1.Pod
		newPodPri     *int32
		shouldSucceed bool
		reason        string
	}{
		{
			name: "high-priority preempts low-priority",
			nodes: []*corev1.Node{
				makeNode("node1", true),
				makeNode("node2", true),
			},
			existingPods: []*corev1.Pod{
				makePod("default", "low-pri-pod", "node1", int32Ptr(100)),
				makePod("default", "mid-pri-pod", "node2", int32Ptr(500)),
			},
			newPodPri:     int32Ptr(1000),
			shouldSucceed: true,
			reason:        "high-priority pod should preempt lowest-priority pod",
		},
		{
			name: "low-priority cannot preempt high-priority",
			nodes: []*corev1.Node{
				makeNode("node1", true),
				makeNode("node2", true),
			},
			existingPods: []*corev1.Pod{
				makePod("default", "high-pri-pod", "node1", int32Ptr(1000)),
				makePod("default", "high-pri-pod2", "node2", int32Ptr(1000)),
			},
			newPodPri:     int32Ptr(100),
			shouldSucceed: false,
			reason:        "low-priority pod cannot preempt high-priority pods",
		},
		{
			name: "priority 0 pod cannot preempt other priority 0 pods",
			nodes: []*corev1.Node{
				makeNode("node1", true),
				makeNode("node2", true),
			},
			existingPods: []*corev1.Pod{
				makePod("default", "no-pri-pod", "node1", nil),
				makePod("default", "no-pri-pod2", "node2", nil),
			},
			newPodPri:     nil, // priority 0
			shouldSucceed: false,
			reason:        "priority 0 pod cannot preempt other priority 0 pods",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := makeScheduler(tt.nodes, tt.existingPods)

			newPod := makePod("default", "new-pod", "", tt.newPodPri)
			node, err := s.pickFreeNode(newPod)

			if tt.shouldSucceed {
				if err != nil {
					t.Errorf("expected success, got error: %v", err)
				}
				if node == "" {
					t.Errorf("expected node assignment, got empty")
				}
			} else {
				if err == nil {
					t.Errorf("expected error, got nil - %s", tt.reason)
				}
			}
		})
	}
}

// Helper functions

func makeNode(name string, ready bool) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:   corev1.NodeReady,
					Status: corev1.ConditionUnknown,
				},
			},
		},
	}
	if ready {
		node.Status.Conditions[0].Status = corev1.ConditionTrue
	}
	return node
}

func makePod(namespace, name string, nodeName string, priority *int32) *corev1.Pod {
	return makePodInNamespace(namespace, name, nodeName, priority)
}

func makePodInNamespace(namespace, name, nodeName string, priority *int32) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: corev1.PodSpec{
			Priority: priority,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	if nodeName != "" {
		pod.Spec.NodeName = nodeName
	}
	return pod
}

func int32Ptr(i int32) *int32 {
	return &i
}

func makeScheduler(nodes []*corev1.Node, pods []*corev1.Pod) *scheduler {
	client := fake.NewSimpleClientset()

	// Add nodes to fake client
	for _, node := range nodes {
		client.CoreV1().Nodes().Create(nil, node, metav1.CreateOptions{})
	}

	// Add pods to fake client
	for _, pod := range pods {
		client.CoreV1().Pods(pod.Namespace).Create(nil, pod, metav1.CreateOptions{})
	}

	// Create a node store with our test nodes
	nodeStore := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, node := range nodes {
		nodeStore.Add(node)
	}

	s := &scheduler{
		client:        client,
		schedulerName: "test-scheduler",
		nodeView:      &simpleStore{store: nodeStore},
		queue:         nil,
	}

	return s
}

// simpleStore wraps a cache.Store for testing
type simpleStore struct {
	store cache.Store
}

func (s *simpleStore) GetStore() cache.Store {
	return s.store
}

func (s *simpleStore) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func (s *simpleStore) AddEventHandlerWithResyncPeriod(handler cache.ResourceEventHandler, resyncPeriod time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func (s *simpleStore) RemoveEventHandler(handle cache.ResourceEventHandlerRegistration) error {
	return nil
}

func (s *simpleStore) AddIndexers(indexers cache.Indexers) error {
	return nil
}

func (s *simpleStore) GetIndexer() cache.Indexer {
	return s.store.(cache.Indexer)
}

func (s *simpleStore) GetController() cache.Controller {
	return nil
}

func (s *simpleStore) Run(stopCh <-chan struct{}) {
}

func (s *simpleStore) HasSynced() bool {
	return true
}

func (s *simpleStore) LastSyncResourceVersion() string {
	return ""
}

func (s *simpleStore) IsStopped() bool {
	return false
}

func (s *simpleStore) SetTransform(transform cache.TransformFunc) error {
	return nil
}

func (s *simpleStore) SetWatchErrorHandler(handler cache.WatchErrorHandler) error {
	return nil
}

func TestGangScheduling(t *testing.T) {
	tests := []struct {
		name          string
		nodes         []*corev1.Node
		pods          []*corev1.Pod
		newPodGroup   string
		shouldSucceed bool
	}{
		{
			name: "all pods in gang pending",
			nodes: []*corev1.Node{
				makeNode("node1", true),
				makeNode("node2", true),
				makeNode("node3", true),
			},
			pods: []*corev1.Pod{
				makePodWithGroup("default", "worker-1", "", "group1"),
				makePodWithGroup("default", "worker-2", "", "group1"),
				makePodWithGroup("default", "worker-3", "", "group1"),
			},
			newPodGroup:   "group1",
			shouldSucceed: true,
		},
		{
			name: "pod without gang",
			nodes: []*corev1.Node{
				makeNode("node1", true),
				makeNode("node2", true),
			},
			pods:          []*corev1.Pod{},
			newPodGroup:   "",
			shouldSucceed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := makeScheduler(tt.nodes, tt.pods)

			var newPod *corev1.Pod
			if tt.newPodGroup != "" {
				newPod = makePodWithGroup("default", "test-pod", "", tt.newPodGroup)
			} else {
				newPod = makePod("default", "test-pod", "", nil)
			}

			ok, err := s.checkGangSchedulable(context.Background(), newPod)

			if tt.shouldSucceed && (!ok || err != nil) {
				t.Errorf("expected gang schedulable, got ok=%v err=%v", ok, err)
			}
		})
	}
}

func makePodWithGroup(namespace, name, nodeName, group string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: corev1.PodSpec{
			Priority: nil,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}
	if nodeName != "" {
		pod.Spec.NodeName = nodeName
	}
	if group != "" {
		pod.Annotations = map[string]string{
			GangGroupAnnotation: group,
		}
	}
	return pod
}
