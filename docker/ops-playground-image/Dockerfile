###############################################################################
# Build Click Count Job
###############################################################################
FROM maven:3.6-jdk-8-slim AS builder
# get explore-flink job and compile it
COPY ./java/explore-flink /opt/explore-flink
WORKDIR /opt/explore-flink
RUN mvn clean install

###############################################################################
# Build Operations Playground Image
###############################################################################
FROM flink:1.11.2-scala_2.12
WORKDIR /opt/flink/usrlib
COPY --from=builder --chown=flink:flink /opt/explore-flink/target/explore-flink.jar /opt/flink/usrlib/explore-flink.jar
RUN ln -fs /opt/flink/usrlib/explore-flink.jar /opt/flink/lib/explore-flink.jar

###############################################################################
# Troubleshooting
###############################################################################
# Restart docker images from scratch
#docker system prune
# Access logs
#docker-compose logs clickevent-generator|client|kafka|zookeeper|jobmanager|taskmanager-01|taskmanager-02|taskmanager-03
