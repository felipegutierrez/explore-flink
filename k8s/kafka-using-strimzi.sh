#!/bin/sh

# From https://strimzi.io/quickstarts/

# Applying Strimzi installation file
kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka

# Apply the `Kafka` Cluster CR file
kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-persistent-single.yaml -n kafka
# Then apply our kafka cluster to replace the existing one with 3 replicas
kubectl apply -n kafka -f kafka-metrics.yaml

kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka

# producer
#kubectl -n kafka run kafka-producer -ti --image=strimzi/kafka:0.19.0-kafka-2.5.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --broker-list my-cluster-kafka-bootstrap:9092 --topic my-topic
# consumer
#kubectl -n kafka run kafka-consumer -ti --image=strimzi/kafka:0.19.0-kafka-2.5.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic my-topic --from-beginning

# assign bootstrap server to an external IP
kubectl get services -o wide -n kafka
NAME                          TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)                      AGE   SELECTOR
my-cluster-kafka-bootstrap    ClusterIP   10.98.121.13    <none>        9091/TCP,9092/TCP,9093/TCP   76m   strimzi.io/cluster=my-cluster,strimzi.io/kind=Kafka,strimzi.io/name=my-cluster-kafka
my-cluster-kafka-brokers      ClusterIP   None            <none>        9091/TCP,9092/TCP,9093/TCP   76m   strimzi.io/cluster=my-cluster,strimzi.io/kind=Kafka,strimzi.io/name=my-cluster-kafka
my-cluster-zookeeper-client   ClusterIP   10.110.38.191   <none>        2181/TCP                     77m   strimzi.io/cluster=my-cluster,strimzi.io/kind=Kafka,strimzi.io/name=my-cluster-zookeeper
my-cluster-zookeeper-nodes    ClusterIP   None            <none>        2181/TCP,2888/TCP,3888/TCP   77m   strimzi.io/cluster=my-cluster,strimzi.io/kind=Kafka,strimzi.io/name=my-cluster-zookeeper

kubectl get service my-cluster-kafka-bootstrap -n kafka -o=jsonpath='{.spec.ports[0].nodePort}{"\n"}'
minikube ip
 172.17.0.2
kubectl get nodes --output=jsonpath='{range .items[*]}{.status.addresses[?(@.type=="172.17.0.2")].address}{"\n"}{end}'

# accessing the Kubernetes dashboard

kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.0.4/aio/deploy/recommended.yaml
kubectl proxy
# access the URL:
http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/#/login
# create the tocken
kubectl create serviceaccount dashboard-admin-sa
kubectl create clusterrolebinding dashboard-admin-sa --clusterrole=cluster-admin --serviceaccount=default:dashboard-admin-sa
kubectl get secrets
# NAME                             TYPE                                  DATA   AGE
# dashboard-admin-sa-token-g44wr   kubernetes.io/service-account-token   3      2m21s
# default-token-7lqvq              kubernetes.io/service-account-token   3      11d

# Copy the token and enter it into the token field on the Kubernetes dashboard login page.
kubectl describe secret dashboard-admin-sa-token-g44wr
# Name:         dashboard-admin-sa-token-g44wr
# Namespace:    default
# Labels:       <none>
# Annotations:  kubernetes.io/service-account.name: dashboard-admin-sa
#               kubernetes.io/service-account.uid: 21a4dc00-0cca-41c0-9991-89f750968564
#
# Type:  kubernetes.io/service-account-token
#
# Data
# ====
# ca.crt:     1066 bytes
# namespace:  7 bytes
# token:      XXXXXXX....XXXX


