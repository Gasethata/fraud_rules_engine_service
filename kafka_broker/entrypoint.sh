#!/bin/bash
#set -e

# echo "Starting Kafka in KRaft mode..."

# if [ -z "$KAFKA_CLUSTER_ID" ]; then
#   export KAFKA_CLUSTER_ID=$(kafka-storage random-uuid)
#   echo "Generated new Kafka CLUSTER_ID: $KAFKA_CLUSTER_ID"
# fi

# export KAFKA_CLUSTER_ID=$(docker run --rm confluentinc/cp-kafka:7.5.0 kafka-storage random-uuid)
# echo "KAFKA_CLUSTER_ID=$KAFKA_CLUSTER_ID" > .env


# echo "Starting Kafka broker..."
# exec /etc/confluent/docker/run
