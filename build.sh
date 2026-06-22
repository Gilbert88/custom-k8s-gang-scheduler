#!/bin/bash
set -e

echo "Building Go binary..."
go build -o scheduler

echo "Building Docker image..."
docker build -t gilbertsong/custom-k8s-scheduler:latest .

echo "Pushing to registry..."
docker push gilbertsong/custom-k8s-scheduler:latest

echo "Done!"
