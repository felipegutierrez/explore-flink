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
FLINK_CLI=/home/flink/flink-1.9.0/bin/flink
FLINK_APP=/home/flink/explore-flink/target/explore-flink.jar
FLINK_START_CLUSTER=/home/flink/flink-1.9.0/bin/start-cluster.sh
FLINK_START_CLUSTER_MESOS=/home/flink/flink-1.9.0/bin/mesos-appmaster.sh

#######################################################################
## Instructions
#######################################################################
echo
echo "${bold}Launching the Flink Standalone cluster:${normal}"
echo "   $FLINK_START_CLUSTER"
echo "${bold}Launching the Flink + Mesos cluster:${normal}"
echo "   $FLINK_START_CLUSTER_MESOS"
echo
echo "${bold}Launching a Flink Stream application >>${normal}"
echo
echo "   $FLINK_CLI run -c org.sense.flink.App $FLINK_APP -app 30 -source 130.239.48.136 -sink 130.239.48.136 -offlineData [true|false] -frequencyPull [seconds] -frequencyWindow [seconds] -parallelism [int] -disableOperatorChaining [true|false] -output [file|mqtt]"
echo "   $FLINK_CLI run -c org.sense.flink.App $FLINK_APP -app 30 -source 130.239.48.136 -sink 130.239.48.136 -offlineData true -frequencyPull 1 -frequencyWindow 30 -parallelism 4 -disableOperatorChaining true -output file"
echo
echo "${bold}description of each parameter:${normal}"
echo "   ${bold}-app :${normal} which application to deploy. If you don't pass any parameter the jar file will output all applications available."
echo "   ${bold}-source,-sink:${normal} IP of the source and sink nodes. It means that you can see the output of the application on the machines that hold these IPs."
echo "   ${bold}-offlineData:${normal} TRUE means to use offline data. FALSE means to download data from Valencia open data portal (http://gobiernoabierto.valencia.es/en/data/)."
echo "   ${bold}-frequencyPull:${normal} frequency of pooling data from the data source in seconds. Online data is downloaded from Valencia open data porta and stored on the local machine. So it is still possible to increate the polling frequency of this data. Usually if you want a very high frequency of polling you could set this parameter for 1 second and increase the polling frequency of synthetic data using the 'SideInput operator' which is listenning to a Mqtt broker:"
echo "      mosquitto_pub -h 127.0.0.1 -t topic-synthetic-frequency-pull -m \"AIR_POLLUTION 500\""
echo "      mosquitto_pub -h 127.0.0.1 -t topic-synthetic-frequency-pull -m \"TRAFFIC_JAM 1000\""
echo "      mosquitto_pub -h 127.0.0.1 -t topic-synthetic-frequency-pull -m \"NOISE 600\""
echo "   ${bold}-frequencyWindow:${normal} frequency to compute the window in seconds."
echo "   ${bold}-parallelism:${normal} degree of parallelism to deploy the application on the cluster. It means the redundante operators will be created in order to guarantee fault tolerance."
echo "   ${bold}-disableOperatorChaining:${normal} FALSE is the default. TRUE disables fusion optimization for all operators which means that operators will be allocated on different threads (https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/config.html#configuring-taskmanager-processing-slots)."
echo "   ${bold}-output:${normal} 'file' means that the output will be generated in the Flink TaskManagers log files. 'mqtt' means that you have to subscribe to a mqtt channel according to the message showed when the application is deployed."
echo

echo
echo "${bold}Consuming a Flink Stream application output >>${normal}"
echo "   mosquitto_sub -h 130.239.48.136 -t topic-valencia-data-cpu-intensive"
echo

echo "${bold}Listing all Flink Stream applications${normal}"
echo "   $FLINK_CLI list"
echo

echo "${bold}Canceling a Flink Stream application${normal}"
echo "   $FLINK_CLI cancel APP_ID"
echo

echo "${bold}Sending parameters to change the frequency of synthetic item generators${normal}"
echo "   mosquitto_pub -h 127.0.0.1 -t topic-synthetic-frequency-pull -m \"AIR_POLLUTION 500\""
echo "   mosquitto_pub -h 127.0.0.1 -t topic-synthetic-frequency-pull -m \"TRAFFIC_JAM 1000\""
echo "   mosquitto_pub -h 127.0.0.1 -t topic-synthetic-frequency-pull -m \"NOISE 600\""
echo


