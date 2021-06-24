#!/bin/bash

# Exit immediately if a *pipeline* returns a non-zero status. (Add -x for command tracing)
set -e

# Copy config files if not provided in volume
cp -rn $KAFKA_HOME/config.orig/* $KAFKA_HOME/config

case $1 in
    zookeeper)
        shift
        # Change the Zookeeper snapshot directory in zookeeper.properities file
        sed -i "s|dataDir=.*|dataDir=${KAFKA_HOME}/data|" $KAFKA_HOME/config/zookeeper.properties
        exec $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
        ;;
    kafka)
        shift
        if [ -z "$1" ]; then
            exec /scripts/kafka-start.sh start
        else
            exec /scripts/kafka-start.sh "$@"        
        fi
        ;;
    kafka-connect)
        shift
        if [ -z "$1" ]; then
            exec /scripts/kafka-connect-start.sh start
        else
            echo "Kafka-connect can not have any arguments other than start"        
        fi
        ;;
    *)    
        echo "First argument must be either zookeeper, kafka or kafka-connect."
        exit 1
        ;;
esac