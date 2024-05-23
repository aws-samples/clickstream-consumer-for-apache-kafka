#!/bin/sh
echo Starting Kafka consumer

echo "${BOOTSTRAP_SERVERS_CONFIG}, ${SCHEMA_REGISTRY_URL_CONFIG}, ${SSL_KEYSTORE_PASSWORD_CONFIG}, ${SSL_KEY_PASSWORD_CONFIG}"
sed -i "s/bootstrap.servers=.*/bootstrap.servers=${BOOTSTRAP_SERVERS_CONFIG}/" consumer.properties
sed -i "s/schema.registry.url=.*/schema.registry.url=${SCHEMA_REGISTRY_URL_CONFIG}/" consumer.properties
sed -i "s/ssl.keystore.password=.*/ssl.keystore.password=${SSL_KEYSTORE_PASSWORD_CONFIG}/" consumer.properties
sed -i "s/ssl.key.password=.*/ssl.key.password=${SSL_KEY_PASSWORD_CONFIG}/" consumer.properties

EXTRA_ARGS=-javaagent:/opt/jmx_prometheus_javaagent-0.13.0.jar=3800:./kafka-producer-consumer.yml
java $EXTRA_ARGS -jar target/KafkaClickstreamConsumer-1.0-SNAPSHOT.jar \
   --topic "${TOPIC}" \
   --propertiesFilePath consumer.properties \
   --numThreads "${NMTRD}" \
   --iamEnable \
   --glueSchemaRegistry \
   --gsrRegion "${REGION}"
