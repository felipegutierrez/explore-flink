
[![Build Status](https://api.travis-ci.org/felipegutierrez/explore-flink.svg?branch=master)](https://travis-ci.org/felipegutierrez/explore-flink)

This project is based on [Apache Flink 1.11.1](https://flink.apache.org/) with docker-compose, Java 8, and Scala 2.12. The docker images can be found at [Docker Hub](https://hub.docker.com/repository/docker/felipeogutierrez/explore-flink). 

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
Troubleshooting:
```
docker-compose logs clickevent-generator|client|kafka|zookeeper|jobmanager|taskmanager-01|taskmanager-02|taskmanager-03
docker-compose images
docker system prune
docker image ls
docker run -i -t felipeogutierrez/tpch-dbgen /bin/bash
```

## Kubernetes

```
$ minikube start
$ minikube ssh 'sudo ip link set docker0 promisc on'
$ kubectl create -f k8s/flink-configuration-configmap.yaml
$ kubectl create -f k8s/jobmanager-service.yaml
$ kubectl create -f k8s/jobmanager-session-deployment.yaml
$ kubectl create -f k8s/taskmanager-session-deployment.yaml
$ kubectl create -f k8s/jobmanager-rest-service.yaml
$ kubectl get deployments
NAME                READY   UP-TO-DATE   AVAILABLE   AGE
flink-jobmanager    1/1     1            1           16m
flink-taskmanager   2/2     2            2           15m
$ kubectl get pods
NAME                                 READY   STATUS    RESTARTS   AGE
flink-jobmanager-fccd97c9c-7s96g     1/1     Running   0          17m
flink-taskmanager-869f5cf7f9-2kkmn   1/1     Running   0          15m
flink-taskmanager-869f5cf7f9-fnk5l   1/1     Running   0          15m
$ kubectl get services
NAME                    TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)                      AGE
flink-jobmanager        ClusterIP   10.111.78.59   <none>        6123/TCP,6124/TCP,8081/TCP   66m
flink-jobmanager-rest   NodePort    10.111.73.83   <none>        8081:30081/TCP               7m12s
kubernetes              ClusterIP   10.96.0.1      <none>        443/TCP                      8d
$ kubectl get svc flink-jobmanager-rest
$ minikube ip
172.17.0.2
```
Access [http://172.17.0.2:30081](http://172.17.0.2:30081).



