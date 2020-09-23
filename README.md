
[![Build Status](https://api.travis-ci.org/felipegutierrez/explore-flink.svg?branch=master)](https://travis-ci.org/felipegutierrez/explore-flink)

This project is based on [Apache Flink 1.11.1](https://flink.apache.org/) with Docker 19.03.8, Kubernetes v1.19.0, minikube v1.13.1, Java 8, and Scala 2.12. The docker images can be found at [Docker Hub](https://hub.docker.com/repository/docker/felipeogutierrez/explore-flink). 

## Kubernetes + Docker + Flink + Prometheus + Grafana

This section aims to deploy the docker images above using minikube v1.13.1 Kubernetes v1.19.0 and Docker 19.03.8. It is based on the official tutorial [Flink with Kubernetes Setup](https://ci.apache.org/projects/flink/flink-docs-stable/ops/deployment/kubernetes.html).

Deploy the common components of FLink cluster in Kubernetes:
```
minikube start
minikube ssh 'sudo ip link set docker0 promisc on'

kubectl create -f k8s/flink-configuration-configmap.yaml
kubectl create -f k8s/jobmanager-service.yaml
kubectl proxy
kubectl create -f k8s/jobmanager-rest-service.yaml
kubectl get svc flink-jobmanager-rest
```
List the objects:
```
$ kubectl get configmaps
NAME           DATA   AGE
flink-config   2      16m
$ kubectl get services
NAME                    TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)                      AGE
flink-jobmanager        ClusterIP   10.111.78.59   <none>        6123/TCP,6124/TCP,8081/TCP   66m
flink-jobmanager-rest   NodePort    10.111.73.83   <none>        8081:30081/TCP               7m12s
kubernetes              ClusterIP   10.96.0.1      <none>        443/TCP                      8d
```
Deploy 2 `PersistentVolumeClaim` to share a directory, a `Job` to create the TPC-H files, 1 `Job` with the Flink-JobManager, and 1 `Deployment` with 3 Flink-TaskManagers in Kubernetes:
```
kubectl create -f k8s/tpch-dbgen-pvc.yaml
kubectl create -f k8s/tpch-dbgen-datarate-pvc.yaml
kubectl create -f k8s/tpch-dbgen-job.yaml
kubectl create -f k8s/jobmanager-job.yaml
kubectl create -f k8s/taskmanager-job-deployment.yaml
```
List the objects:
```
kubectl get jobs
kubectl get pvc
kubectl get deployments
kubectl get pods

$ kubectl get jobs
NAME               COMPLETIONS   DURATION   AGE
flink-jobmanager   0/1           75s        75s
tpch-dbgen-job     0/1           2m55s      2m55s
$ kubectl get pvc
NAME                      STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   AGE
tpch-dbgen-data-pvc       Bound    pvc-eb57cdbe-ff81-4ba7-85b6-675e2bead9ff   200Mi      RWO            standard       3h10m
tpch-dbgen-datarate-pvc   Bound    pvc-ae76640d-c689-443f-b516-0850ca657fa2   1Ki        RWO            standard       19m
$ kubectl get deployments
NAME                READY   UP-TO-DATE   AVAILABLE   AGE
flink-taskmanager   3/3     3            3           78s
$ kubectl get pods
NAME                                 READY   STATUS    RESTARTS   AGE
flink-jobmanager-27qvq               1/1     Running   0          95s
flink-taskmanager-5c95bcc75b-4dp6n   1/1     Running   0          85s
flink-taskmanager-5c95bcc75b-h5x8p   1/1     Running   0          85s
flink-taskmanager-5c95bcc75b-rllc4   1/1     Running   0          85s
tpch-dbgen-job-z5pqj                 1/1     Running   0          3m15s
```
Use the minikube IP address `minikube ip` to access the Flink UI-Web at [http://172.17.0.2:30081](http://172.17.0.2:30081).

### Troubleshooting:
Logs:
```
$ kubectl describe pod <POD_ID>
$ kubectl logs <POD_ID>
$ kubectl exec -i -t <POD_ID> -- /bin/bash
```
Clean your Kubernetes cluster and delete everything when you finish to test.
```
$ kubectl delete jobs flink-jobmanager
$ kubectl delete deployments flink-jobmanager flink-taskmanager
$ kubectl delete services flink-jobmanager flink-jobmanager-rest
$ kubectl delete configmaps flink-config
$ minikube stop
```

## Docker + Flink + Prometheus + Grafana
This section is here only to help if one would like to not use Kubernetes and deploy Flink only using Docker.
```
cd operations-playground
docker-compose build --no-cache
docker-compose up -d --remove-orphans
```
 - WebUI Flink: [http://127.0.0.1:8081/](http://127.0.0.1:8081/).
 - Prometheus console: [http://127.0.0.1:9090/](http://127.0.0.1:9090/).
 - Grafana dashboard: [http://127.0.0.1:3000/](http://127.0.0.1:3000/).
 - List the images running:
```
docker-compose ps -a
                    Name                                  Command               State                   Ports                
-----------------------------------------------------------------------------------------------------------------------------
operations-playground_clickevent-generator_1   /docker-entrypoint.sh java ...   Up       6123/tcp, 8081/tcp                  
operations-playground_client_1                 /docker-entrypoint.sh flin ...   Exit 0                                       
operations-playground_jobmanager_1             /docker-entrypoint.sh jobm ...   Up       6123/tcp, 0.0.0.0:8081->8081/tcp    
operations-playground_kafka_1                  start-kafka.sh                   Up       0.0.0.0:9094->9094/tcp              
operations-playground_taskmanager-01_1         /docker-entrypoint.sh task ...   Up       6123/tcp, 8081/tcp                  
operations-playground_taskmanager-02_1         /docker-entrypoint.sh task ...   Up       6123/tcp, 8081/tcp                  
operations-playground_taskmanager-03_1         /docker-entrypoint.sh task ...   Up       6123/tcp, 8081/tcp                  
operations-playground_zookeeper_1              /bin/sh -c /usr/sbin/sshd  ...   Up       2181/tcp, 22/tcp, 2888/tcp, 3888/tcp
```
Stop the images:
```
docker-compose down
```

### Troubleshooting:
```
docker-compose logs clickevent-generator|client|kafka|zookeeper|jobmanager|taskmanager-01|taskmanager-02|taskmanager-03
docker-compose images
docker container prune
docker system prune
docker image ls
docker image rm ID
docker run -i -t felipeogutierrez/tpch-dbgen /bin/bash
```

