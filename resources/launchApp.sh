#!/bin/bash


FLINK_HOME=/home/flink/flink-1.9.0
FLINK_CLI=/home/flink/flink-1.9.0/bin/flink
FLINK_APP=/home/flink/app/explore-flink.jar

# launching a Flink stream application
# ./bin/flink run -c org.sense.flink.App ../app/explore-flink.jar 30 127.0.0.1 192.168.56.1
OUTPUT=$(echo `$FLINK_CLI run -c org.sense.flink.App $FLINK_APP -app 30 -source 127.0.0.1 -sink 127.0.0.1 -offlineData true -frequencyPull 10 -frequencyWindow 30 -syntheticData true 2>&1`)

echo "${OUTPUT}"

# echo `$FLINK_CLI list`

