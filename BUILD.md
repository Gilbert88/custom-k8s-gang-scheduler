# Building the Custom K8s Scheduler

## Important: Network Requirements

The scheduler requires downloading Go modules from public mirrors (proxy.golang.org). **This environment has network restrictions that block these downloads.** You must build in an environment with unrestricted network access to the public Go proxy.

## Build Options

### Option 1: Use GitHub Actions (Recommended)
Push to the repo and let the workflow at `.github/workflows/build.yml` build and push the image.

### Option 2: Build Locally (Requires network access)
In an environment with unrestricted network (not on this corporate network):

```bash
go build -o scheduler
docker build -t gilbertsong/custom-k8s-scheduler:latest .
```

### Option 3: Build with Docker (Requires network access)

```bash
docker build -t gilbertsong/custom-k8s-scheduler:latest .
```

The image requires network access to download Go modules during the build.

## Deploying to Kubernetes

Apply the scheduler deployment:

```bash
kubectl apply -f scheduler-deployment.yaml
```

Then schedule a pod with this scheduler:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: test-pod
spec:
  schedulerName: dumb-scheduler
  containers:
  - name: busybox
    image: busybox
    command: ["sleep", "3600"]
```

## How It Works

The scheduler places pods one per node. It:
1. Watches for pods requesting `schedulerName=dumb-scheduler`
2. Finds a node with no running pods (one pod per node model)
3. Binds the pod to that free node
4. Emits a Scheduled event

It does not account for CPU, memory, or any other resources.

## Code Structure

- `main.go` — Entry point, kubeconfig handling, informer setup
- `schedule.go` — Core scheduling logic (node selection, binding, events)
- `signal.go` — Graceful shutdown on SIGINT/SIGTERM
- `go.mod` — Module dependencies
- `Dockerfile` — Multi-stage Docker build
- `.github/workflows/build.yml` — CI/CD pipeline

