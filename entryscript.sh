#!/bin/bash

SPARK_LOAD=$1

echo "SPARK_LOAD: $SPARK_LOAD"

/etc/init.d/ssh start

if [ "$SPARK_LOAD" == "master" ]; then
    echo "Formatting HDFS..."
    hdfs namenode -format

    echo "Starting NameNode, SecondaryNameNode, and ResourceManager..."
    hdfs --daemon start namenode
    hdfs --daemon start secondarynamenode
    yarn --daemon start resourcemanager

    if ! hdfs dfs -test -d /spark-logs; then
        while ! hdfs dfs -mkdir -p /spark-logs; do
            echo "Failed creating /spark-logs hdfs dir"
        done
        echo "/spark-logs directory created successfully."
    else
        echo "/spark-logs HDFS dir already exists, skipping creation."
    fi

    if ! hdfs dfs -test -d /opt/spark-data; then
        echo "Creating HDFS data dir..."
        hdfs dfs -mkdir -p /opt/spark-data
    else
        echo "/opt/spark-data HDFS dir already exists, skipping creation."
    fi

    echo "Copying local data to HDFS..."
    hdfs dfs -copyFromLocal /opt/spark/data/* /opt/spark-data
    hdfs dfs -ls /opt/spark-data

elif [ "$SPARK_LOAD" == "worker" ]; then
    echo "Starting DataNode and NodeManager..."
    hdfs --daemon start datanode
    yarn --daemon start nodemanager

elif [ "$SPARK_LOAD" == "logs" ]; then
    while ! hdfs dfs -test -d /spark-logs; do
        echo "Spark logs not available yet. Retrying..."
        sleep 2
    done
    echo "Spark logs available. Starting History Server..."
    start-history-server.sh
fi

tail -f /dev/null
