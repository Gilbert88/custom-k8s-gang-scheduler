# custom-k8s-gang-scheduler

A minimal K8s scheduler with one-pod-per-node placement, preemption support with PriorityClass, and gang scheduling with preemption support.

## Dumb scheduler key info

- One pod per node constraint
- Priority-based preemption
- Gang scheduling with pod groups via pod annotation, with preemption support

### Create Cluster (not using minikube b/c without docker desktop, the open source docker/engine has incompatible 'docker version' output with minikube)

```bash
kind create cluster --config kind-config.yaml --name test
```

### Deploy Scheduler (replace kube-system below for a diff ns)

```bash
NAMESPACE=kube-system envsubst < scheduler-deployment.yaml | kubectl apply -f -
```

### Deploy Test Pods

```bash
kubectl apply -f test-pod.yaml
kubectl apply -f test-preemption.yaml
kubectl apply -f test-gang.yaml
kubectl apply -f test-gang-preemption.yaml
```

### Run Unit Tests

```bash
go test -v
```

### Next: scheduling retry mechanism

it would take some more time to support retry logics. we need:
1. a work queue with backoff logics
2. auto re-queue failed pod
3. gang pod group should be all or nothing - that means if one pod fails in a gang, all pods in the gang pod group should be retry
4. retry cannot be forever - configurable maximum retry or exponantial retry
5. retry should also be respecting the priorityClass - that means the queue is not first come first serve, it is priority based first then FCFS

### Next Next: Large scale concerns

for any large scale batch workloads, which can possible dump 20k+ pods with different combination of gang sizes to the scheduler. likely this custom scheduler will be slow or stuck for a long time. To deal with this batch case, we should:
1. instead of existing query from apiserver, we should use the informer cache, which is an upstream k8s library
2. similar to database, in scheduler, we can also index the node->pod mapping for quicker lookup
3. gang preemption might be common if the cluster is resource limited. we should optimize the preemption algorithm to find out potential victims more efficiently - instead of scan over all nodes and all pods, we can maintain more mapping based on each of the priority class.
4. we should do bin-packing on the likely bottleneck resource (like GPU) - k8s scheduler is pluggable architecture. we can add a bin-pack plugin to improve the resource allocation efficiency.
5. if the node count is large, e.g., 10k+ k8s worker nodes, we should add data structure like priority queue for quicker sorting of the worker nodes.
6. if worker node count is 10k+, networking can also be bottleneck. there are diff ways to configure the DNS, kube-proxy and etcd to ensure bigger scale. and on scheduler side, we can support pre-calculated decisions, and explore if we could partition scheduler into multiple instances and figure out how multiple schedulers can sync their states. 
7. last, it is the multi-cluster coordination or federation across regions. there is existing projects like Kqueue that could support multi-cluster coordination (e.g., when a cluster is full, route upcoming workloads to another cluster).
