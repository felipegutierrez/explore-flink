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
FROM flink:1.11.1-scala_2.12
#WORKDIR /opt/flink/bin
#COPY --from=builder /opt/explore-flink/target/explore-flink.jar /opt/explore-flink.jar
#COPY --from=builder /opt/explore-flink/target/explore-flink.jar ./explore-flink.jar
WORKDIR /opt/flink/usrlib
COPY --from=builder /opt/explore-flink/target/explore-flink.jar /opt/flink/usrlib/explore-flink.jar
ADD /opt/flink/usrlib/explore-flink.jar /opt/flink/usrlib/explore-flink.jar
#USER flink

###############################################################################
# Troubleshooting
###############################################################################
# Restart docker images from scratch
#docker system prune
# Access logs
#docker-compose logs clickevent-generator|client|kafka|zookeeper|jobmanager|taskmanager-01|taskmanager-02|taskmanager-03
