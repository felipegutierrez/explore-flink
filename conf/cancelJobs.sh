#!/bin/bash
#set -x
#set -v

FLINK_CLI=/home/flink/flink-1.9.0/bin/flink

wget "http://127.0.0.1:8081/jobs/" -O jobsID.json

for k in $(jq -c '.jobs | .[]' jobsID.json); do
	echo "================================"
	#echo $k
	STATUS=$(echo $k | jq -c '.status' | sed "s/\"//g")
	ID=$(echo $k | jq -c '.id' | sed "s/\"//g")
	echo "ID: $ID STATUS: $STATUS"
	if [ "$STATUS" = "RUNNING" ]; then
		read -p "Are you sure you want to cancel JobID $i? (y/n)" -n 1 -r
		if [[ $REPLY =~ ^[Yy]$ ]]; then
			echo "   ... canceling Job ID $ID"
			echo `$FLINK_CLI cancel $ID`
		fi
	fi
done


