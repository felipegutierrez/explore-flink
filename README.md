
[![Build Status](https://travis-ci.com/felipegutierrez/explore-flink.svg?branch=master)](https://travis-ci.com/felipegutierrez/explore-flink)
[![codecov](https://codecov.io/gh/felipegutierrez/explore-flink/branch/master/graph/badge.svg?token=MFG0YKQT25)](https://codecov.io/gh/felipegutierrez/explore-flink)
![GitHub issues](https://img.shields.io/github/issues-raw/felipegutierrez/explore-flink)
![GitHub closed issues](https://img.shields.io/github/issues-closed-raw/felipegutierrez/explore-flink)
![Docker Image Size (latest by date)](https://img.shields.io/docker/image-size/felipeogutierrez/explore-flink)
![Lines of code](https://img.shields.io/tokei/lines/github/felipegutierrez/explore-flink)


This project is based on [Apache Flink 1.11.2](https://flink.apache.org/) consuming events from Kafka 2.2.5 (using [Strimzi](https://strimzi.io/quickstarts/) operators) with Docker 19.03.8, Kubernetes v1.19.0, minikube v1.13.1, Java 8, and Scala 2.12. The docker images can be found at [Docker Hub](https://hub.docker.com/repository/docker/felipeogutierrez/explore-flink). 

## 1. Kubernetes + Docker + Kafka & Zookeeper (3 brokers & 3 zookeepers from [Strimzi](https://strimzi.io/quickstarts/) operators) + Flink(1 JobManager & 3 TaskManagers) + Prometheus + Grafana

This section aims to use Flink consuming data from Kafka and from a filesystem, exporting data to Promethues and displaying it at Grafana dashboard. It is based on the official tutorial [Flink with Kubernetes Setup](https://ci.apache.org/projects/flink/flink-docs-stable/ops/deployment/kubernetes.html).
```
minikube start --cpus 4 --memory 8192
```
First configure Kafka using the Strimzi operators based on this [file](k8s/kafka-using-strimzi.sh). Then, apply the other yaml files available on directory [k8s/](k8s/).
```
minikube ssh 'sudo ip link set docker0 promisc on'
kubectl proxy
kubectl apply -n kafka -f k8s/
```
Use the minikube IP address `minikube ip` to access the Flink UI-Web at [http://172.17.0.2:30081](http://172.17.0.2:30081), the Prometheus WebUI at [http://172.17.0.2:30091/targets](http://172.17.0.2:30091/targets) and [http://172.17.0.2:30091/graph](http://172.17.0.2:30091/graph), and the Grafana WebUI at [http://172.17.0.2:30011](http://172.17.0.2:30011).

### Overview of this project in action

![Kafka(Strimzi) - Flink web UI - prometheus - grafana - using Kubernetes](images/screencast-00.gif)

### Troubleshooting
Testing Kafka client:
```
$ kubectl exec -it kafka-client -- /bin/bash
root@kafka-client:/# cd /usr/bin/
root@kafka-client:/usr/bin# kafka-topics --list --zookeeper zookeeper:2181
__confluent.support.metrics
```
List the objects, the resources:
```
kubectl get all
kubectl api-resources
```
Logs:
```
kubectl describe pod <POD_ID>
kubectl logs <POD_ID>
kubectl exec -i -t <POD_ID> -- /bin/bash
kubectl -n kube-system top pods
kubectl get nodes
kubectl top nodes
kubectl get pods -A
kubectl top pods
```
Clean your Kubernetes cluster and delete everything when you finish to test.
```
kubectl delete statefulset flink-taskmanager kafka zookeeper
kubectl delete jobs flink-jobmanager tpch-dbgen-job
kubectl delete deployments kafka-broker grafana-deployment prometheus-deployment tpch-dbgen-deployment
kubectl delete services zookeeper kafka flink-jobmanager flink-jobmanager-rest flink-taskmanager prometheus-service-rest prometheus-service grafana-service-rest grafana-service
kubectl delete pvc tpch-dbgen-data-pvc tpch-dbgen-datarate-pvc
kubectl delete configmaps flink-config grafana-config prometheus-config
kubectl delete pods kafka-client

minikube stop
```

## 2. Docker + Kafka(1 ZooKeeper, 1 broker) + Flink(1JobManager, 3TaskManagers) + Prometheus + Grafana
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

### Troubleshooting
```
docker-compose logs clickevent-generator|client|kafka|zookeeper|jobmanager|taskmanager-01|taskmanager-02|taskmanager-03
docker-compose images
docker container prune
docker system prune
docker image ls
docker image rm ID
docker run -i -t felipeogutierrez/tpch-dbgen /bin/bash
```

