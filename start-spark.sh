#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" == "master" ];
then
  echo "Starting Spark Master..."
  start-master.sh -p 7077
elif [ "$SPARK_WORKLOAD" == "worker" ];
then
  echo "Starting Spark Worker..."
  start-worker.sh spark://spark-master:7077
elif [ "$SPARK_WORKLOAD" == "history" ]
then
  echo "Starting Spark History Server..."
  start-history-server.sh
else
  echo "Invalid SPARK_WORKLOAD: $SPARK_WORKLOAD"
  echo "Usage: $0 {master|worker|history}"
  exit 1
fi