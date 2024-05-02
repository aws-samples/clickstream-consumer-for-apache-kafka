#!/bin/sh
echo Starting Kafka consumer

echo "${BOOTSTRAP_SERVERS_CONFIG}, ${SCHEMA_REGISTRY_URL_CONFIG}, ${SSL_KEYSTORE_PASSWORD_CONFIG}, ${SSL_KEY_PASSWORD_CONFIG}"
sed -i "s/BOOTSTRAP_SERVERS_CONFIG=.*/BOOTSTRAP_SERVERS_CONFIG=${BOOTSTRAP_SERVERS_CONFIG}/" consumer.properties
sed -i "s/SCHEMA_REGISTRY_URL_CONFIG=.*/SCHEMA_REGISTRY_URL_CONFIG=${SCHEMA_REGISTRY_URL_CONFIG}/" consumer.properties
sed -i "s/SSL_KEYSTORE_PASSWORD_CONFIG=.*/SSL_KEYSTORE_PASSWORD_CONFIG=${SSL_KEYSTORE_PASSWORD_CONFIG}/" consumer.properties
sed -i "s/SSL_KEY_PASSWORD_CONFIG=.*/SSL_KEY_PASSWORD_CONFIG=${SSL_KEY_PASSWORD_CONFIG}/" consumer.properties

EXTRA_ARGS=-javaagent:/opt/jmx_prometheus_javaagent-0.13.0.jar=3800:/opt/kafka-producer-consumer.yml
java $EXTRA_ARGS -jar target/KafkaClickstreamConsumer-1.0-SNAPSHOT.jar \
   --topic "${TOPIC}" \
   --propertiesFilePath consumer.properties \
   --numThreads "${NMTRD}" \
   --iamEnable \
   --glueSchemaRegistry \
   --gsrRegion "${REGION}"
