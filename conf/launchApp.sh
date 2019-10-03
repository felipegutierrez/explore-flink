#!/bin/bash
#set -x
#set -v

#######################################################################
## some variables
#######################################################################
bold=$(tput bold)
normal=$(tput sgr0)

#######################################################################
## FLink variables
#######################################################################
FLINK_HOME=/home/flink/flink-1.9.0
FLINK_JAR=/home/flink/flink-1.9.0/lib/flink-dist_2.11-1.9.0.jar
FLINK_CLI=/home/flink/flink-1.9.0/bin/flink
FLINK_APP=/home/flink/explore-flink/target/explore-flink.jar
FLINK_START_CLUSTER=/home/flink/flink-1.9.0/bin/start-cluster.sh
FLINK_START_CLUSTER_MESOS=/home/flink/flink-1.9.0/bin/mesos-appmaster.sh

#######################################################################
## Instructions
#######################################################################
## Launch data producers
#######################################################################
echo
echo "${bold}Launching producers${normal}"
echo "application 32 is a mqtt producer for traffic jam data from Valencia Open-data web portal"
echo "application 33 is a mqtt producer for noise data from Valencia Open-data web portal"
echo "intructions of how to change the frequency of producing data will be present when the command is issued"
echo "java -classpath ${FLINK_JAR}:${FLINK_APP} org.sense.flink.App -app 32 -offlineData true &"
echo "java -classpath ${FLINK_JAR}:${FLINK_APP} org.sense.flink.App -app 33 -offlineData true &"
echo
echo "${bold}Sending parameters to change the frequency (milliseconds) of synthetic item generators${normal}"
echo "   mosquitto_pub -h localhost -p 1883 -t topic-valencia-traffic-jam-frequency -m '10000'"
echo "   mosquitto_pub -h localhost -p 1883 -t topic-valencia-pollution-frequency -m '5000'"
echo
echo "${bold}Launching the Flink Standalone cluster:${normal}"
echo "   $FLINK_START_CLUSTER"
echo "${bold}Launching the Flink + Mesos cluster:${normal}"
echo "   $FLINK_START_CLUSTER_MESOS"
echo
echo "${bold}Launching a Flink Stream application >>${normal}"
echo
echo "   $FLINK_CLI run -c org.sense.flink.App $FLINK_APP -app 34 -source 130.239.48.136 -sink 130.239.48.136 -frequencyWindow [seconds] -parallelism [int] -disableOperatorChaining [true|false] -output [file|mqtt] &"
echo "   $FLINK_CLI run -c org.sense.flink.App $FLINK_APP -app 34 -source 130.239.48.136 -sink 130.239.48.136 -frequencyWindow 60 -parallelism 4 -disableOperatorChaining true -output mqtt &"
echo
echo "${bold}description of each parameter:${normal}"
echo "   ${bold}-app :${normal} which application to deploy. If you don't pass any parameter the jar file will output all applications available."
echo "   ${bold}-source,-sink:${normal} IP of the source and sink nodes. It means that you can see the output of the application on the machines that hold these IPs."
echo "   ${bold}-frequencyWindow:${normal} frequency to compute the window in seconds."
echo "   ${bold}-parallelism:${normal} degree of parallelism to deploy the application on the cluster. It means the redundante operators will be created in order to guarantee fault tolerance."
echo "   ${bold}-disableOperatorChaining:${normal} FALSE is the default. TRUE disables fusion optimization for all operators which means that operators will be allocated on different threads (https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/config.html#configuring-taskmanager-processing-slots)."
echo "   ${bold}-output:${normal} 'file' means that the output will be generated in the Flink TaskManagers log files. 'mqtt' means that you have to subscribe to a mqtt channel according to the message showed when the application is deployed."
echo

echo
echo "${bold}Consuming a Flink Stream application output >>${normal}"
echo "You can see on the mqtt broker or on the log output file of the task manager"
echo "   mosquitto_sub -h 130.239.48.136 -t topic-valencia-cpu-intensive"
echo

echo "${bold}Listing all Flink Stream applications${normal}"
echo "   $FLINK_CLI list"
echo

echo "${bold}Canceling a Flink Stream application${normal}"
echo "   $FLINK_CLI cancel APP_ID"
echo

echo "${bold}Sending parameters to change the frequency of synthetic item generators${normal}"
echo "   mosquitto_pub -h localhost -p 1883 -t topic-valencia-traffic-jam-frequency -m '1000'"
echo "   mosquitto_pub -h localhost -p 1883 -t topic-valencia-pollution-frequency -m '100'"
echo
echo


