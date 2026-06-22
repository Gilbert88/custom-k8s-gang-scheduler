package main

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

const GangGroupAnnotation = "gang-scheduler.io/pod-group"

func getPodGroup(pod *corev1.Pod) string {
	if pod.Annotations == nil {
		return ""
	}
	return pod.Annotations[GangGroupAnnotation]
}

func (s *scheduler) checkGangSchedulable(ctx context.Context, pod *corev1.Pod) (bool, error) {
	groupName := getPodGroup(pod)
	if groupName == "" {
		return true, nil
	}

	pods, err := s.client.CoreV1().Pods(pod.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("listing pods for gang check: %w", err)
	}

	var gangPods []*corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		if getPodGroup(p) == groupName {
			gangPods = append(gangPods, p)
		}
	}

	if len(gangPods) == 0 {
		return false, fmt.Errorf("no pods found in gang %q", groupName)
	}

	for _, p := range gangPods {
		if p.Spec.NodeName != "" {
			continue
		}
		if p.Status.Phase != corev1.PodPending {
			klog.V(4).Infof("pod %s/%s in gang %q has phase %v, not schedulable", p.Namespace, p.Name, groupName, p.Status.Phase)
			return false, nil
		}
	}

	klog.V(4).Infof("gang %q: all %d pods are in valid state for scheduling", groupName, len(gangPods))
	return true, nil
}

func (s *scheduler) getAllNodes() []*corev1.Node {
	var nodes []*corev1.Node
	for _, obj := range s.nodeView.GetStore().List() {
		if node, ok := obj.(*corev1.Node); ok {
			if nodeSchedulable(node) {
				nodes = append(nodes, node)
			}
		}
	}
	return nodes
}

func (s *scheduler) emitGangScheduledEvent(ctx context.Context, pod *corev1.Pod, node string) {
	groupName := getPodGroup(pod)
	if groupName == "" {

		return
	}

	pods, err := s.client.CoreV1().Pods(pod.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return
	}

	scheduled := 0
	total := 0
	for i := range pods.Items {
		p := &pods.Items[i]
		if getPodGroup(p) == groupName {
			total++
			if p.Spec.NodeName != "" {
				scheduled++
			}
		}
	}

	if scheduled == total {
		klog.Infof("gang %q: all %d pods scheduled", groupName, total)
	}
}
