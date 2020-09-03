FROM maven:3.6-jdk-8-slim AS builder

COPY ./src /opt/explore-flink/
COPY ./pom.xml /opt/explore-flink/
COPY ./conf /opt/explore-flink/
WORKDIR /opt/explore-flink
RUN mvn clean package

FROM flink:1.11.1-scala_2.12-java8
WORKDIR /opt/flink/bin
#COPY --from=builder /opt/explore-flink/target/explore-flink.jar /opt/explore-flink.jar
#WORKDIR .
#COPY ../pom.xml /tmp/explore-flink/
#COPY ../src /tmp/explore-flink/
#COPY ../conf /tmp/explore-flink/
#WORKDIR /tmp/explore-flink
#RUN mvn clean install

#FROM flink:1.11.1-scala_2.12-java8
#WORKDIR /opt/flink/bin
