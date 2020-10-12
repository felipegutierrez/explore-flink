#!/bin/sh

# From https://strimzi.io/quickstarts/

# Applying Strimzi installation file
kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka

# Apply the `Kafka` Cluster CR file
kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-persistent-single.yaml -n kafka

kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka

# producer
kubectl -n kafka run kafka-producer -ti --image=strimzi/kafka:0.19.0-kafka-2.5.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --broker-list my-cluster-kafka-bootstrap:9092 --topic my-topic
# consumer
kubectl -n kafka run kafka-consumer -ti --image=strimzi/kafka:0.19.0-kafka-2.5.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic my-topic --from-beginning

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

