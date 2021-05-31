
minikube start
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
kubectl exec hello-world-0 -- curl hello-world-1.hello-world.default.svc.cluster.local:8080
kubectl exec hello-world-0 -- env