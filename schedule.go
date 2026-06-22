package main

import (
	"context"
	"fmt"
	"math"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

func (s *scheduler) enqueuePodIfPending(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	if !needsScheduling(pod, s.schedulerName) {
		return
	}
	key, err := cache.MetaNamespaceKeyFunc(pod)
	if err != nil {
		klog.Errorf("computing pod key: %v", err)
		return
	}
	s.queue.Add(key)
}

func needsScheduling(pod *corev1.Pod, schedulerName string) bool {
	return pod.Spec.SchedulerName == schedulerName &&
		pod.Spec.NodeName == "" &&
		pod.Status.Phase == corev1.PodPending &&
		pod.DeletionTimestamp == nil
}

func (s *scheduler) worker(ctx context.Context) {
	for s.processNext(ctx) {
	}
}

func (s *scheduler) processNext(ctx context.Context) bool {
	key, quit := s.queue.Get()
	if quit {
		return false
	}
	defer s.queue.Done(key)

	keyStr, ok := key.(string)
	if !ok {
		klog.Errorf("queue item is not a string: %T", key)
		return true
	}

	if err := s.schedule(ctx, keyStr); err != nil {
		klog.Errorf("scheduling %q failed, will retry: %v", keyStr, err)
		s.queue.AddRateLimited(key)
		return true
	}
	s.queue.Forget(key)
	return true
}

func (s *scheduler) schedule(ctx context.Context, key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return fmt.Errorf("splitting key: %w", err)
	}

	obj, exists, err := s.podsView.GetIndexer().GetByKey(key)
	if err != nil {
		return fmt.Errorf("looking up pod: %w", err)
	}
	if !exists {
		klog.V(4).Infof("pod %q gone before scheduling; dropping", key)
		return nil
	}
	pod := obj.(*corev1.Pod)
	if !needsScheduling(pod, s.schedulerName) {
		klog.V(4).Infof("pod %q no longer needs scheduling; dropping", key)
		return nil
	}

	node, err := s.pickFreeNode(pod)
	if err != nil {
		return err
	}

	gangOk, err := s.checkGangSchedulable(ctx, pod)
	if err != nil {
		return err
	}
	if !gangOk {
		return fmt.Errorf("not all pods in gang are ready to schedule")
	}

	klog.Infof("binding pod %s/%s to node %q", namespace, name, node)
	return s.bind(ctx, pod, node)
}

func (s *scheduler) pickFreeNode(pod *corev1.Pod) (string, error) {
	occupied := s.occupiedNodes()
	gangExcluded := s.getNodesWithGangPods(pod)
	groupName := getPodGroup(pod)
	isGangPod := groupName != ""

	for _, obj := range s.nodeView.GetStore().List() {
		node := obj.(*corev1.Node)
		if !nodeSchedulable(node) {
			continue
		}
		if !occupied[node.Name] && !gangExcluded[node.Name] {
			return node.Name, nil
		}
	}

	if isGangPod {
		gangsNeeded, err := s.countUnscheduledGangPods(pod.Namespace, groupName)
		if err != nil {
			return "", fmt.Errorf("counting gang pods: %w", err)
		}

		nodesWithVictims := s.findNodesForGangPreemption(pod, gangsNeeded)
		if len(nodesWithVictims) == gangsNeeded {
			for _, nodeName := range nodesWithVictims {
				s.preemptFromNode(pod.Namespace, nodeName)
			}
			return nodesWithVictims[0], nil
		}

		return "", fmt.Errorf("no free node available for gang pod")
	}

	podPri := s.podPriority(pod)

	victimNode := s.findPreemptionCandidate(podPri)
	if victimNode == "" {
		return "", fmt.Errorf("no free node and no lower-priority pod to preempt")
	}

	klog.Infof("preempting a lower-priority pod on node %q to schedule %s/%s", victimNode, pod.Namespace, pod.Name)
	return victimNode, nil
}

func (s *scheduler) countUnscheduledGangPods(namespace, groupName string) (int, error) {
	pods, err := s.client.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return 0, err
	}

	count := 0
	for i := range pods.Items {
		p := &pods.Items[i]
		if getPodGroup(p) == groupName && p.Spec.NodeName == "" {
			count++
		}
	}
	return count, nil
}

func (s *scheduler) findNodesForGangPreemption(pod *corev1.Pod, needed int) []string {
	gangExcluded := s.getNodesWithGangPods(pod)

	pods, err := s.client.CoreV1().Pods(metav1.NamespaceAll).List(context.Background(), metav1.ListOptions{
		FieldSelector: "status.phase!=Failed,status.phase!=Succeeded",
	})
	if err != nil {
		return nil
	}

	podPri := s.podPriority(pod)
	minPri := int32(0)
	if podPri != nil {
		minPri = *podPri
	}

	type nodeCandidate struct {
		nodeName string
		pod      *corev1.Pod
		priority int32
	}

	nodeVictims := make(map[string]*nodeCandidate)

	for i := range pods.Items {
		p := &pods.Items[i]

		ns := p.Namespace
		if ns == "kube-system" || ns == "kube-public" || ns == "kube-node-lease" || ns == "kube-apiserver" {
			continue
		}

		if p.Spec.NodeName == "" {
			continue
		}

		if gangExcluded[p.Spec.NodeName] {
			continue
		}

		podPri := int32(0)
		if p.Spec.Priority != nil {
			podPri = *p.Spec.Priority
		}

		if podPri >= minPri {
			continue
		}

		if existing, ok := nodeVictims[p.Spec.NodeName]; ok {
			if podPri < existing.priority {
				nodeVictims[p.Spec.NodeName] = &nodeCandidate{
					nodeName: p.Spec.NodeName,
					pod:      p,
					priority: podPri,
				}
			}
		} else {
			nodeVictims[p.Spec.NodeName] = &nodeCandidate{
				nodeName: p.Spec.NodeName,
				pod:      p,
				priority: podPri,
			}
		}
	}

	result := make([]string, 0, needed)
	for _, candidate := range nodeVictims {
		if len(result) >= needed {
			break
		}
		result = append(result, candidate.nodeName)
	}

	return result
}

func (s *scheduler) preemptFromNode(namespace, nodeName string) {
	pods, err := s.client.CoreV1().Pods(metav1.NamespaceAll).List(context.Background(), metav1.ListOptions{
		FieldSelector: "status.phase!=Failed,status.phase!=Succeeded",
	})
	if err != nil {
		klog.Errorf("listing pods for preemption: %v", err)
		return
	}

	var victimPod *corev1.Pod
	var lowestPriority int32 = math.MaxInt32

	for i := range pods.Items {
		p := &pods.Items[i]

		if p.Spec.NodeName != nodeName {
			continue
		}

		ns := p.Namespace
		if ns == "kube-system" || ns == "kube-public" || ns == "kube-node-lease" || ns == "kube-apiserver" {
			continue
		}

		podPri := int32(0)
		if p.Spec.Priority != nil {
			podPri = *p.Spec.Priority
		}

		if podPri < lowestPriority {
			lowestPriority = podPri
			victimPod = p
		}
	}

	if victimPod != nil {
		if err := s.client.CoreV1().Pods(victimPod.Namespace).Delete(context.Background(), victimPod.Name, metav1.DeleteOptions{}); err != nil {
			klog.Errorf("deleting victim pod %s/%s: %v", victimPod.Namespace, victimPod.Name, err)
		}
	}
}

func (s *scheduler) getNodesWithGangPods(pod *corev1.Pod) map[string]bool {
	groupName := getPodGroup(pod)
	if groupName == "" {
		return map[string]bool{}
	}

	nodesWithGangPods := map[string]bool{}
	pods, err := s.client.CoreV1().Pods(pod.Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		klog.Errorf("listing pods to find gang nodes: %v", err)
		return nodesWithGangPods
	}

	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Name != pod.Name && getPodGroup(p) == groupName && p.Spec.NodeName != "" {
			nodesWithGangPods[p.Spec.NodeName] = true
		}
	}

	return nodesWithGangPods
}

func (s *scheduler) occupiedNodes() map[string]bool {
	occupied := map[string]bool{}
	pods, err := s.client.CoreV1().Pods(metav1.NamespaceAll).List(context.Background(), metav1.ListOptions{
		FieldSelector: "status.phase!=Failed,status.phase!=Succeeded",
	})
	if err != nil {
		klog.Errorf("listing pods to compute occupancy: %v", err)
		return occupied
	}
	for i := range pods.Items {

		ns := pods.Items[i].Namespace
		if ns == "kube-system" || ns == "kube-public" || ns == "kube-node-lease" || ns == "kube-apiserver" {
			continue
		}
		if node := pods.Items[i].Spec.NodeName; node != "" {
			occupied[node] = true
		}
	}
	return occupied
}

func nodeSchedulable(node *corev1.Node) bool {
	if node.Spec.Unschedulable {
		return false
	}
	for _, taint := range node.Spec.Taints {
		if taint.Effect == corev1.TaintEffectNoSchedule || taint.Effect == corev1.TaintEffectNoExecute {
			return false
		}
	}
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return true
}

func (s *scheduler) bind(ctx context.Context, pod *corev1.Pod, node string) error {
	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pod.Namespace,
			Name:      pod.Name,
			UID:       pod.UID,
		},
		Target: corev1.ObjectReference{
			Kind: "Node",
			Name: node,
		},
	}
	if err := s.client.CoreV1().Pods(pod.Namespace).Bind(ctx, binding, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("binding pod to node %q: %w", node, err)
	}
	s.emitScheduledEvent(ctx, pod, node)
	s.emitGangScheduledEvent(ctx, pod, node)
	return nil
}

func (s *scheduler) podPriority(pod *corev1.Pod) *int32 {
	if pod.Spec.Priority != nil {
		return pod.Spec.Priority
	}
	return nil
}

func (s *scheduler) findPreemptionCandidate(minPriority *int32) string {
	pods, err := s.client.CoreV1().Pods(metav1.NamespaceAll).List(context.Background(), metav1.ListOptions{
		FieldSelector: "status.phase!=Failed,status.phase!=Succeeded",
	})
	if err != nil {
		klog.Errorf("listing pods for preemption: %v", err)
		return ""
	}

	minPri := int32(0)
	if minPriority != nil {
		minPri = *minPriority
	}

	var victimPod *corev1.Pod
	var victimNode string
	var lowestPriority int32 = minPri

	for i := range pods.Items {
		p := &pods.Items[i]

		ns := p.Namespace
		if ns == "kube-system" || ns == "kube-public" || ns == "kube-node-lease" || ns == "kube-apiserver" {
			continue
		}
		if p.Spec.NodeName == "" {
			continue
		}

		podPri := int32(0)
		if p.Spec.Priority != nil {
			podPri = *p.Spec.Priority
		}

		if podPri < lowestPriority {
			lowestPriority = podPri
			victimPod = p
			victimNode = p.Spec.NodeName
		}
	}

	if victimPod != nil {

		if err := s.client.CoreV1().Pods(victimPod.Namespace).Delete(context.Background(), victimPod.Name, metav1.DeleteOptions{}); err != nil {
			klog.Errorf("deleting victim pod %s/%s: %v", victimPod.Namespace, victimPod.Name, err)
			return ""
		}
		return victimNode
	}

	return ""
}

func (s *scheduler) emitScheduledEvent(ctx context.Context, pod *corev1.Pod, node string) {
	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: pod.Name + "-",
			Namespace:    pod.Namespace,
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Namespace: pod.Namespace,
			Name:      pod.Name,
			UID:       pod.UID,
		},
		Reason:  "Scheduled",
		Message: fmt.Sprintf("Successfully assigned %s/%s to %s", pod.Namespace, pod.Name, node),
		Source:  corev1.EventSource{Component: s.schedulerName},
		Type:    corev1.EventTypeNormal,
		FirstTimestamp: metav1.Now(),
		LastTimestamp:  metav1.Now(),
		Count:          1,
	}
	if _, err := s.client.CoreV1().Events(pod.Namespace).Create(ctx, event, metav1.CreateOptions{}); err != nil {
		klog.V(4).Infof("could not emit Scheduled event for %s/%s: %v", pod.Namespace, pod.Name, err)
	}
}
