#!/bin/sh

# Strating Kafka consumer

echo -e "Current file contents:\n $(cat /etc/hosts)"
echo "$DETECTED_IP $DETECTED_HOSTNAME" >> /etc/hosts
echo -e "\n\n\nUpdated file contents:\n $(cat /etc/hosts)"

echo "brokers: [$BROKERS]"
sed -i "s/BROKERS/${BROKERS}/g" /opt/consumer.properties

echo Starting Kafka consumer

cd /opt

EXTRA_ARGS=-javaagent:/opt/jmx_prometheus_javaagent-0.13.0.jar=3800:/opt/kafka-producer-consumer.yml
java $EXTRA_ARGS -jar KafkaClickstreamConsumer-1.0-SNAPSHOT.jar -t $TOPIC -pfp /opt/consumer.properties -nt $NMTRD -gsr -gsrr $REGION -iam