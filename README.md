
[![Build Status](https://api.travis-ci.org/felipegutierrez/explore-flink.svg?branch=master)](https://travis-ci.org/felipegutierrez/explore-flink)

This project is based on [Apache Flink 1.11.1](https://flink.apache.org/) with Docker 19.03.8, Kubernetes v1.19.0, minikube v1.13.1, Java 8, and Scala 2.12. The docker images can be found at [Docker Hub](https://hub.docker.com/repository/docker/felipeogutierrez/explore-flink). 

## Kubernetes + Docker + Flink + Prometheus + Grafana

This section aims to deploy the docker images above using minikube v1.13.1 Kubernetes v1.19.0 and Docker 19.03.8. It is based on the official tutorial [Flink with Kubernetes Setup](https://ci.apache.org/projects/flink/flink-docs-stable/ops/deployment/kubernetes.html).

Deploy the common components of FLink cluster in Kubernetes:
```
$ minikube start
$ minikube ssh 'sudo ip link set docker0 promisc on'

$ kubectl create -f k8s/flink-configuration-configmap.yaml
$ kubectl get configmaps
NAME           DATA   AGE
flink-config   2      16m

$ kubectl create -f k8s/jobmanager-service.yaml
$ kubectl proxy
$ kubectl create -f k8s/jobmanager-rest-service.yaml
$ kubectl get svc flink-jobmanager-rest
$ kubectl get services
NAME                    TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)                      AGE
flink-jobmanager        ClusterIP   10.111.78.59   <none>        6123/TCP,6124/TCP,8081/TCP   66m
flink-jobmanager-rest   NodePort    10.111.73.83   <none>        8081:30081/TCP               7m12s
kubernetes              ClusterIP   10.96.0.1      <none>        443/TCP                      8d
```
Deploy the 3 task managers and one job manager within a stream application in Kubernetes:
```
$ kubectl create -f k8s/jobmanager-job.yaml
$ kubectl get jobs
NAME               COMPLETIONS   DURATION   AGE
flink-jobmanager   0/1           3m16s      3m16s

$ kubectl create -f k8s/taskmanager-job-pvc.yaml
$ kubectl get pvc
NAME                  STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   AGE
tpch-dbgen-data-pvc   Bound    pvc-004508b4-d224-4094-976a-43ab62b3fdce   200Mi      RWO            standard       26s

$ kubectl create -f k8s/taskmanager-job-deployment.yaml
$ kubectl get deployments
NAME                READY   UP-TO-DATE   AVAILABLE   AGE
flink-taskmanager   3/3     3            3           3m33s
$ kubectl get pods
NAME                                 READY   STATUS    RESTARTS   AGE
flink-jobmanager-zj8tw               1/1     Running   0          2m58s
flink-taskmanager-765584b699-422g9   1/1     Running   0          2m56s
flink-taskmanager-765584b699-8cmdq   1/1     Running   0          2m56s
flink-taskmanager-765584b699-gb865   1/1     Running   0          2m56s

$ minikube ip
172.17.0.2
```
Access [http://172.17.0.2:30081](http://172.17.0.2:30081). 

### Troubleshooting:
Logs:
```
$ kubectl get pods
NAME                     READY   STATUS              RESTARTS   AGE
flink-jobmanager-kl92f   0/1     ContainerCreating   0          2s
$ kubectl describe pod flink-jobmanager-kl92f
$ kubectl logs flink-jobmanager-kl92f
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

