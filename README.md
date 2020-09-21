
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

This sesion aims to deploy the docker images above using minikube v1.13.0 Kubernetes v1.19.0 and Docker 19.03.8. It is based on the official tutorial [Flink with Kubernetes Setup](https://ci.apache.org/projects/flink/flink-docs-stable/ops/deployment/kubernetes.html).
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

```
$ kubectl create -f k8s/jobmanager-job.yaml
$ kubectl get jobs
NAME               COMPLETIONS   DURATION   AGE
flink-jobmanager   0/1           88s        88s

$ kubectl create -f k8s/taskmanager-job-deployment.yaml
$ kubectl get deployments
NAME                READY   UP-TO-DATE   AVAILABLE   AGE
flink-taskmanager   3/3     3            3           25s
$ kubectl get pods
NAME                               READY   STATUS             RESTARTS   AGE
flink-jobmanager-tfnzk             0/1     CrashLoopBackOff   4          3m9s
flink-taskmanager-578dcdcd-9vpc7   1/1     Running            0          40s
flink-taskmanager-578dcdcd-g7jxf   1/1     Running            0          40s
flink-taskmanager-578dcdcd-vzwxc   1/1     Running            0          40s
```
Access [http://172.17.0.2:30081](http://172.17.0.2:30081).
```
$ kubectl create -f k8s/jobmanager-session-deployment.yaml
$ kubectl create -f k8s/taskmanager-session-deployment.yaml
$ kubectl get deployments
NAME                READY   UP-TO-DATE   AVAILABLE   AGE
flink-jobmanager    1/1     1            1           16m
flink-taskmanager   2/2     2            2           15m
$ kubectl get pods
NAME                                 READY   STATUS              RESTARTS   AGE
flink-jobmanager-6c8f8b98b4-7srkd    0/1     ContainerCreating   0          21s
flink-taskmanager-796c9b948d-dh2fn   0/1     ContainerCreating   0          16s
flink-taskmanager-796c9b948d-pgp7j   0/1     ContainerCreating   0          16s
flink-taskmanager-796c9b948d-vrn2r   0/1     ContainerCreating   0          16s

$ minikube ip
172.17.0.2
```
Troubleshooting:
```
$ kubectl get pods
NAME                     READY   STATUS              RESTARTS   AGE
flink-jobmanager-kl92f   0/1     ContainerCreating   0          2s

$ kubectl describe pod flink-jobmanager-kl92f
Name:         flink-jobmanager-kl92f
Namespace:    default
Priority:     0
Node:         minikube/172.17.0.2
Start Time:   Fri, 18 Sep 2020 10:31:29 +0200
Labels:       app=flink
              component=jobmanager
              controller-uid=8fc3fc5e-d497-4d38-963a-f3c3c063fefc
              job-name=flink-jobmanager
Annotations:  <none>
Status:       Running
IP:           172.18.0.6
IPs:
  IP:           172.18.0.6
Controlled By:  Job/flink-jobmanager
Containers:
  jobmanager:
    Container ID:  docker://05dc29f847b37d55b2bb40470e9a41303c9031ecda59b566b3db637412deb9b4
    Image:         flink:1.11.0-scala_2.12
    Image ID:      docker-pullable://flink@sha256:665db47d0a2bcc297e9eb4df7640d3e4c1d398d25849252a726c8ada112722cf
    Ports:         6123/TCP, 6124/TCP, 8081/TCP
    Host Ports:    0/TCP, 0/TCP, 0/TCP
    Args:
      standalone-job
      --job-classname
      org.apache.flink.streaming.examples.wordcount.WordCount
    State:          Running
      Started:      Fri, 18 Sep 2020 10:31:51 +0200
    Last State:     Terminated
      Reason:       Error
      Exit Code:    1
      Started:      Fri, 18 Sep 2020 10:31:37 +0200
      Finished:     Fri, 18 Sep 2020 10:31:39 +0200
    Ready:          True
    Restart Count:  2
    Liveness:       tcp-socket :6123 delay=30s timeout=1s period=60s #success=1 #failure=3
    Environment:    <none>
    Mounts:
      /opt/flink/conf from flink-config-volume (rw)
      /opt/flink/usrlib from job-artifacts-volume (rw)
      /var/run/secrets/kubernetes.io/serviceaccount from default-token-djb2t (ro)
Conditions:
  Type              Status
  Initialized       True 
  Ready             True 
  ContainersReady   True 
  PodScheduled      True 
Volumes:
  flink-config-volume:
    Type:      ConfigMap (a volume populated by a ConfigMap)
    Name:      flink-config
    Optional:  false
  job-artifacts-volume:
    Type:          HostPath (bare host directory volume)
    Path:          /host/path/to/job/artifacts
    HostPathType:  
  default-token-djb2t:
    Type:        Secret (a volume populated by a Secret)
    SecretName:  default-token-djb2t
    Optional:    false
QoS Class:       BestEffort
Node-Selectors:  <none>
Tolerations:     node.kubernetes.io/not-ready:NoExecute op=Exists for 300s
                 node.kubernetes.io/unreachable:NoExecute op=Exists for 300s
Events:
  Type     Reason     Age                   From               Message
  ----     ------     ----                  ----               -------
  Normal   Scheduled  2m41s                                    Successfully assigned default/flink-jobmanager-kl92f to minikube
  Normal   Pulled     66s (x5 over 2m38s)   kubelet, minikube  Container image "flink:1.11.0-scala_2.12" already present on machine
  Normal   Created    65s (x5 over 2m37s)   kubelet, minikube  Created container jobmanager
  Normal   Started    65s (x5 over 2m36s)   kubelet, minikube  Started container jobmanager
  Warning  BackOff    36s (x10 over 2m29s)  kubelet, minikube  Back-off restarting failed container

$ kubectl get pods
NAME                               READY   STATUS             RESTARTS   AGE
flink-jobmanager-kl92f             0/1     CrashLoopBackOff   5          5m31s
flink-taskmanager-578dcdcd-f27xw   1/1     Running            0          54s
flink-taskmanager-578dcdcd-hhj89   1/1     Running            0          54s
flink-taskmanager-578dcdcd-hjzjq   1/1     Running            0          54s

$ kubectl logs flink-jobmanager-kl92f
Starting Job Manager
sed: couldn't open temporary file /opt/flink/conf/sedHY9IiP: Read-only file system
sed: couldn't open temporary file /opt/flink/conf/sedW9Bi9P: Read-only file system
/docker-entrypoint.sh: 72: /docker-entrypoint.sh: cannot create /opt/flink/conf/flink-conf.yaml: Permission denied
/docker-entrypoint.sh: 91: /docker-entrypoint.sh: cannot create /opt/flink/conf/flink-conf.yaml.tmp: Read-only file system
Starting standalonejob as a console application on host flink-jobmanager-v79jx.
2020-09-18 08:40:20,156 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] - --------------------------------------------------------------------------------
2020-09-18 08:40:20,159 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -  Preconfiguration: 
2020-09-18 08:40:20,159 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] - 


JM_RESOURCE_PARAMS extraction logs:
jvm_params: -Xmx1073741824 -Xms1073741824 -XX:MaxMetaspaceSize=268435456
logs: INFO  [] - Loading configuration property: jobmanager.rpc.address, flink-jobmanager
INFO  [] - Loading configuration property: taskmanager.numberOfTaskSlots, 4
INFO  [] - Loading configuration property: blob.server.port, 6124
INFO  [] - Loading configuration property: jobmanager.rpc.port, 6123
INFO  [] - Loading configuration property: taskmanager.rpc.port, 6122
INFO  [] - Loading configuration property: queryable-state.proxy.ports, 6125
INFO  [] - Loading configuration property: jobmanager.memory.process.size, 1600m
INFO  [] - Loading configuration property: taskmanager.memory.process.size, 1728m
INFO  [] - Loading configuration property: parallelism.default, 2
INFO  [] - The derived from fraction jvm overhead memory (160.000mb (167772162 bytes)) is less than its min value 192.000mb (201326592 bytes), min value will be used instead
INFO  [] - Final Master Memory configuration:
INFO  [] -   Total Process Memory: 1.563gb (1677721600 bytes)
INFO  [] -     Total Flink Memory: 1.125gb (1207959552 bytes)
INFO  [] -       JVM Heap:         1024.000mb (1073741824 bytes)
INFO  [] -       Off-heap:         128.000mb (134217728 bytes)
INFO  [] -     JVM Metaspace:      256.000mb (268435456 bytes)
INFO  [] -     JVM Overhead:       192.000mb (201326592 bytes)

2020-09-18 08:40:20,160 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] - --------------------------------------------------------------------------------
2020-09-18 08:40:20,160 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -  Starting StandaloneApplicationClusterEntryPoint (Version: 1.11.0, Scala: 2.12, Rev:d04872d, Date:2020-06-29T16:13:14+02:00)
2020-09-18 08:40:20,160 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -  OS current user: flink
2020-09-18 08:40:20,161 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -  Current Hadoop/Kerberos user: <no hadoop dependency found>
2020-09-18 08:40:20,161 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -  JVM: OpenJDK 64-Bit Server VM - Oracle Corporation - 1.8/25.262-b10
2020-09-18 08:40:20,161 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -  Maximum heap size: 989 MiBytes
2020-09-18 08:40:20,161 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -  JAVA_HOME: /usr/local/openjdk-8
2020-09-18 08:40:20,161 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -  No Hadoop Dependency available
2020-09-18 08:40:20,161 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -  JVM Options:
2020-09-18 08:40:20,162 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -     -Xmx1073741824
2020-09-18 08:40:20,162 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -     -Xms1073741824
2020-09-18 08:40:20,162 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -     -XX:MaxMetaspaceSize=268435456
2020-09-18 08:40:20,162 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -     -Dlog.file=/opt/flink/log/flink--standalonejob-0-flink-jobmanager-v79jx.log
2020-09-18 08:40:20,162 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -     -Dlog4j.configuration=file:/opt/flink/conf/log4j-console.properties
2020-09-18 08:40:20,162 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -     -Dlog4j.configurationFile=file:/opt/flink/conf/log4j-console.properties
2020-09-18 08:40:20,162 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -     -Dlogback.configurationFile=file:/opt/flink/conf/logback-console.xml
2020-09-18 08:40:20,163 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -  Program Arguments:
2020-09-18 08:40:20,163 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -     --configDir
2020-09-18 08:40:20,163 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -     /opt/flink/conf
2020-09-18 08:40:20,163 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -     --job-classname
2020-09-18 08:40:20,163 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -     org.apache.flink.streaming.examples.wordcount.WordCount
2020-09-18 08:40:20,163 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -  Classpath: /opt/flink/lib/flink-csv-1.11.0.jar:/opt/flink/lib/flink-json-1.11.0.jar:/opt/flink/lib/flink-shaded-zookeeper-3.4.14.jar:/opt/flink/lib/flink-table-blink_2.12-1.11.0.jar:/opt/flink/lib/flink-table_2.12-1.11.0.jar:/opt/flink/lib/log4j-1.2-api-2.12.1.jar:/opt/flink/lib/log4j-api-2.12.1.jar:/opt/flink/lib/log4j-core-2.12.1.jar:/opt/flink/lib/log4j-slf4j-impl-2.12.1.jar:/opt/flink/lib/flink-dist_2.12-1.11.0.jar:::
2020-09-18 08:40:20,164 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] - --------------------------------------------------------------------------------
2020-09-18 08:40:20,165 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] - Registered UNIX signal handlers for [TERM, HUP, INT]
2020-09-18 08:40:20,206 ERROR org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] - Could not create application program.
org.apache.flink.util.FlinkException: Could not find the provided job class (org.apache.flink.streaming.examples.wordcount.WordCount) in the user lib directory (/opt/flink/usrlib).
	at org.apache.flink.client.deployment.application.ClassPathPackagedProgramRetriever.getJobClassNameOrScanClassPath(ClassPathPackagedProgramRetriever.java:140) ~[flink-dist_2.12-1.11.0.jar:1.11.0]
	at org.apache.flink.client.deployment.application.ClassPathPackagedProgramRetriever.getPackagedProgram(ClassPathPackagedProgramRetriever.java:123) ~[flink-dist_2.12-1.11.0.jar:1.11.0]
	at org.apache.flink.container.entrypoint.StandaloneApplicationClusterEntryPoint.getPackagedProgram(StandaloneApplicationClusterEntryPoint.java:110) ~[flink-dist_2.12-1.11.0.jar:1.11.0]
	at org.apache.flink.container.entrypoint.StandaloneApplicationClusterEntryPoint.main(StandaloneApplicationClusterEntryPoint.java:78) [flink-dist_2.12-1.11.0.jar:1.11.0]
```

Clean your Kubernetes cluster and delete everything when you finish to test.
```
$ kubectl delete deployments flink-jobmanager flink-taskmanager
$ kubectl delete services flink-jobmanager flink-jobmanager-rest
$ kubectl delete configmaps flink-config
$ minikube stop
```
