#!/bin/bash
#set -x
#set -v

#######################################################################
## some variables
#######################################################################
bold=$(tput bold)
normal=$(tput sgr0)
green=$(tput setaf 2)
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
## Launch Flink cluster
echo
echo "${green}${bold}Launching the Flink Standalone cluster >>${normal}"
echo "   $FLINK_START_CLUSTER"
echo "${green}${bold}Launching the Flink + Mesos cluster >>${normal}"
echo "   $FLINK_START_CLUSTER_MESOS"
echo
#######################################################################
## Launch Flink stream application
echo " ${bold}${green}Launching a Flink Stream application >>${normal}"
echo "   $FLINK_CLI run -c org.sense.flink.App $FLINK_APP -app 34 -source 130.239.48.136 -sink 130.239.48.136 -frequencyWindow [seconds] -parallelism [int] -disableOperatorChaining [true|false] -pinningPolicy [true|false] -output [file|mqtt] &"
echo "${green} Example: CPU intensive application >>${normal}"
echo "   $FLINK_CLI run -c org.sense.flink.App $FLINK_APP -app 34 -source 130.239.48.136 -sink 130.239.48.136 -frequencyWindow 60 -parallelism 4 -disableOperatorChaining true -pinningPolicy true -output mqtt &"
echo
echo "${bold}description of each parameter:${normal}"
echo "   ${bold}-app :${normal} which application to deploy. If you don't pass any parameter the jar file will output all applications available."
echo "   ${bold}-source,-sink:${normal} IP of the source and sink nodes. It means that you can see the output of the application on the machines that hold these IPs."
echo "   ${bold}-frequencyWindow:${normal} frequency to compute the window in seconds."
echo "   ${bold}-parallelism:${normal} degree of parallelism to deploy the application on the cluster. It means the redundante operators will be created in order to guarantee fault tolerance."
echo "   ${bold}-disableOperatorChaining:${normal} FALSE is the default. TRUE disables fusion optimization for all operators which means that operators will be allocated on different threads (https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/config.html#configuring-taskmanager-processing-slots)."
echo "   ${bold}-pinningPolicy:${normal} TRUE enables the strategy to pinning operator' threads to specific CPU cores. FALSE is the deault."
echo "   ${bold}-output:${normal} 'file' means that the output will be generated in the Flink TaskManagers log files. 'mqtt' means that you have to subscribe to a mqtt channel according to the message showed when the application is deployed."
echo
#######################################################################
## Launch data producers
echo "${green}${bold}Launching producers >>${normal}"
echo " application 32 is a mqtt producer for traffic jam data from Valencia Open-data web portal"
echo " application 33 is a mqtt producer for noise data from Valencia Open-data web portal"
echo "${green} Example >>${normal}"
echo "java -classpath ${FLINK_JAR}:${FLINK_APP} org.sense.flink.App -app 32 -offlineData true -maxCount 100 &"
echo "java -classpath ${FLINK_JAR}:${FLINK_APP} org.sense.flink.App -app 33 -offlineData true -maxCount 100 &"
echo
echo "${green}${bold} Sending parameters to change the frequency (milliseconds) of synthetic item generators >>${normal}"
echo " Intructions of how to change the frequency of producing data"
echo "${green} Example >>${normal}"
echo "   mosquitto_pub -h localhost -p 1883 -t topic-valencia-traffic-jam-frequency -m '1000'"
echo "   mosquitto_pub -h localhost -p 1883 -t topic-valencia-pollution-frequency -m '5000'"
echo
#######################################################################
## Checking producers
echo "${bold}Check if the producers are sending data to the MQTT broker >>${normal}"
echo "You can see on the mqtt broker or on the log output file of the task manager"
echo "   mosquitto_sub -h 130.239.48.136 -t topic-valencia-cpu-intensive"
echo
#######################################################################
## Listing Flink spplication
echo "${bold}Listing all Flink Stream applications${normal}"
echo "   $FLINK_CLI list"
echo
#######################################################################
## Cancelling Flink application
echo "${bold}Canceling a Flink Stream application${normal}"
echo "   $FLINK_CLI cancel APP_ID"
echo

echo
