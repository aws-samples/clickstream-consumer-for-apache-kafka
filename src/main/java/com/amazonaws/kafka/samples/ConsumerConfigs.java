package com.amazonaws.kafka.samples;

import com.amazonaws.kafka.samples.saslscram.Secrets;
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

class ConsumerConfigs {

    private static String BOOTSTRAP_SERVERS_CONFIG = "http://127.0.0.1:9092";
    private static final String SCHEMA_REGISTRY_URL_CONFIG = "http://127.0.0.1:8081";
    private static final String VALUE_DESERIALIZER_CLASS_CONFIG = "io.confluent.kafka.serializers.KafkaAvroDeserializer";
    private static final String KEY_DESERIALIZER_CLASS_CONFIG = "io.confluent.kafka.serializers.KafkaAvroDeserializer";
    private static final String SSL_TRUSTSTORE_LOCATION_CONFIG = "/tmp/kafka.client.truststore.jks";
    private static final String SSL_KEYSTORE_LOCATION_CONFIG = "/tmp/kafka.client.keystore.jks";
    private static final String SSL_KEYSTORE_PASSWORD_CONFIG = "password";
    private static final String SSL_KEY_PASSWORD_CONFIG = "password";
    private static final String GROUP_ID_CONFIG = "mm2TestConsumer1";
    private static final String METADATA_MAX_AGE_CONFIG = "60000";
    private static final String CLIENT_ID_CONFIG = "clickstream-consumer";
    private static final String ENABLE_AUTO_COMMIT_CONFIG = "false";
    private static final String AUTO_OFFSET_RESET_CONFIG = "earliest";

    private static boolean sslEnable = false;
    private static boolean mTLSEnable = false;
    private static boolean saslScramEnable = false;
    private static boolean iamEnable = false;
    private static boolean glueSchemaRegistry = false;

    private static final Logger logger = LogManager.getLogger(ConsumerConfigs.class);

    private static String getSaslScramString() {
        String secretNamePrefix = "AmazonMSK_";
        String secret = Secrets.getSecret(secretNamePrefix + KafkaClickstreamConsumer.saslscramUser, Secrets.getSecretsManagerClient(KafkaClickstreamConsumer.region));
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode;
        try {
            jsonNode = objectMapper.readTree(secret);
        } catch (IOException e) {
            logger.error("Error reading returned secret for user {} \n", KafkaClickstreamConsumer.saslscramUser);
            logger.error(Util.stackTrace(e));
            throw new RuntimeException(String.format("Error reading returned secret for user %s \n", KafkaClickstreamConsumer.saslscramUser));
        }
        String password = jsonNode.get("password").asText();
        return "org.apache.kafka.common.security.scram.ScramLoginModule required username=" + KafkaClickstreamConsumer.saslscramUser + " password=" + password + ";";
    }

    static Properties consumerConfig() {
        if (KafkaClickstreamConsumer.mTLSEnable) {
            mTLSEnable = true;
            sslEnable = true;
        } else {
            sslEnable = KafkaClickstreamConsumer.sslEnable;
        }

        if (KafkaClickstreamConsumer.saslscramEnable) {
            saslScramEnable = true;
        }

        if (KafkaClickstreamConsumer.iamEnable) {
            iamEnable = true;
        }

        if (KafkaClickstreamConsumer.glueSchemaRegistry){
            glueSchemaRegistry = true;
        }


        Properties consumerProps = new Properties();
        Properties loadProps = new Properties();
        consumerProps.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_CONFIG);
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID_CONFIG);
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT_CONFIG);

        try (FileInputStream file = new FileInputStream(KafkaClickstreamConsumer.propertiesFilePath)) {
            loadProps.load(file);
            consumerProps.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, loadProps.getProperty("BOOTSTRAP_SERVERS_CONFIG", BOOTSTRAP_SERVERS_CONFIG).equals("") ? BOOTSTRAP_SERVERS_CONFIG : loadProps.getProperty("BOOTSTRAP_SERVERS_CONFIG", BOOTSTRAP_SERVERS_CONFIG));
            consumerProps.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, loadProps.getProperty("GROUP_ID_CONFIG", GROUP_ID_CONFIG).equals("") ? GROUP_ID_CONFIG : loadProps.getProperty("GROUP_ID_CONFIG", GROUP_ID_CONFIG));
            consumerProps.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.METADATA_MAX_AGE_CONFIG, loadProps.getProperty("METADATA_MAX_AGE_CONFIG", METADATA_MAX_AGE_CONFIG).equals("") ? METADATA_MAX_AGE_CONFIG : loadProps.getProperty("METADATA_MAX_AGE_CONFIG", METADATA_MAX_AGE_CONFIG));

            if (sslEnable) {
                consumerProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
                consumerProps.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, loadProps.getProperty("SSL_TRUSTSTORE_LOCATION_CONFIG", SSL_TRUSTSTORE_LOCATION_CONFIG).equals("") ? SSL_TRUSTSTORE_LOCATION_CONFIG : loadProps.getProperty("SSL_TRUSTSTORE_LOCATION_CONFIG", SSL_TRUSTSTORE_LOCATION_CONFIG));
            }
            if (mTLSEnable) {
                consumerProps.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, loadProps.getProperty("SSL_KEYSTORE_LOCATION_CONFIG", SSL_KEYSTORE_LOCATION_CONFIG).equals("") ? SSL_KEYSTORE_LOCATION_CONFIG : loadProps.getProperty("SSL_KEYSTORE_LOCATION_CONFIG", SSL_KEYSTORE_LOCATION_CONFIG));
                consumerProps.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, loadProps.getProperty("SSL_KEYSTORE_PASSWORD_CONFIG", SSL_KEYSTORE_PASSWORD_CONFIG).equals("") ? SSL_KEYSTORE_PASSWORD_CONFIG : loadProps.getProperty("SSL_KEYSTORE_PASSWORD_CONFIG", SSL_KEYSTORE_PASSWORD_CONFIG));
                consumerProps.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, loadProps.getProperty("SSL_KEY_PASSWORD_CONFIG", SSL_KEY_PASSWORD_CONFIG).equals("") ? SSL_KEY_PASSWORD_CONFIG : loadProps.getProperty("SSL_KEY_PASSWORD_CONFIG", SSL_KEY_PASSWORD_CONFIG));
            }
            if (saslScramEnable) {
                consumerProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
                consumerProps.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
                consumerProps.setProperty(SaslConfigs.SASL_JAAS_CONFIG, getSaslScramString());
            }

            if (iamEnable) {
                consumerProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
                consumerProps.setProperty(SaslConfigs.SASL_MECHANISM, "AWS_MSK_IAM");
                consumerProps.setProperty(SaslConfigs.SASL_JAAS_CONFIG, "software.amazon.msk.auth.iam.IAMLoginModule required;");
                consumerProps.setProperty(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
            }

            if (glueSchemaRegistry){
                consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AWSKafkaAvroDeserializer.class.getName());
                consumerProps.setProperty(AWSSchemaRegistryConstants.AWS_REGION, KafkaClickstreamConsumer.gsrRegion);
                consumerProps.setProperty(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName());
                if (KafkaClickstreamConsumer.gsrRegistryName != null)
                    consumerProps.setProperty(AWSSchemaRegistryConstants.REGISTRY_NAME, KafkaClickstreamConsumer.gsrRegistryName);
                if (KafkaClickstreamConsumer.gsrSchemaName != null)
                    consumerProps.setProperty(AWSSchemaRegistryConstants.SCHEMA_NAME, KafkaClickstreamConsumer.gsrSchemaName);
                if (KafkaClickstreamConsumer.gsrSchemaDescription != null)
                    consumerProps.setProperty(AWSSchemaRegistryConstants.DESCRIPTION, KafkaClickstreamConsumer.gsrSchemaDescription);
                if (KafkaClickstreamConsumer.secondaryDeserializer)
                    consumerProps.setProperty(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER, KafkaAvroDeserializer.class.getName());
            } else {
                consumerProps.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, loadProps.getProperty("VALUE_DESERIALIZER_CLASS_CONFIG", VALUE_DESERIALIZER_CLASS_CONFIG).equals("") ? VALUE_DESERIALIZER_CLASS_CONFIG : loadProps.getProperty("VALUE_DESERIALIZER_CLASS_CONFIG", VALUE_DESERIALIZER_CLASS_CONFIG));
                consumerProps.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, loadProps.getProperty("KEY_DESERIALIZER_CLASS_CONFIG", KEY_DESERIALIZER_CLASS_CONFIG).equals("") ? KEY_DESERIALIZER_CLASS_CONFIG : loadProps.getProperty("KEY_SERIALIZER_CLASS_CONFIG", KEY_DESERIALIZER_CLASS_CONFIG));
            }
            consumerProps.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, loadProps.getProperty("SCHEMA_REGISTRY_URL_CONFIG", SCHEMA_REGISTRY_URL_CONFIG).equals("") ? SCHEMA_REGISTRY_URL_CONFIG : loadProps.getProperty("SCHEMA_REGISTRY_URL_CONFIG", SCHEMA_REGISTRY_URL_CONFIG));
            consumerProps.setProperty(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");


        } catch (IOException e) {
            logger.info("Properties file not found in location: {}, using defaults \n", KafkaClickstreamConsumer.propertiesFilePath);
            consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
            consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
            consumerProps.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, METADATA_MAX_AGE_CONFIG);

            if (sslEnable) {
                consumerProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
                consumerProps.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, SSL_TRUSTSTORE_LOCATION_CONFIG);
            }
            if (mTLSEnable) {
                consumerProps.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, SSL_KEYSTORE_LOCATION_CONFIG);
                consumerProps.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, SSL_KEYSTORE_PASSWORD_CONFIG);
                consumerProps.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, SSL_KEY_PASSWORD_CONFIG);
            }
            if (saslScramEnable) {
                consumerProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
                consumerProps.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
                consumerProps.setProperty(SaslConfigs.SASL_JAAS_CONFIG, getSaslScramString());
            }
            if (iamEnable) {
                consumerProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
                consumerProps.setProperty(SaslConfigs.SASL_MECHANISM, "AWS_MSK_IAM");
                consumerProps.setProperty(SaslConfigs.SASL_JAAS_CONFIG, "software.amazon.msk.auth.iam.IAMLoginModule required;");
                consumerProps.setProperty(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
            }

            if (glueSchemaRegistry){
                consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AWSKafkaAvroDeserializer.class.getName());
                consumerProps.setProperty(AWSSchemaRegistryConstants.AWS_REGION, KafkaClickstreamConsumer.gsrRegion);
                consumerProps.setProperty(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());
                if (KafkaClickstreamConsumer.gsrRegistryName != null)
                    consumerProps.setProperty(AWSSchemaRegistryConstants.REGISTRY_NAME, KafkaClickstreamConsumer.gsrRegistryName);
                if (KafkaClickstreamConsumer.gsrSchemaName != null)
                    consumerProps.setProperty(AWSSchemaRegistryConstants.SCHEMA_NAME, KafkaClickstreamConsumer.gsrSchemaName);
                if (KafkaClickstreamConsumer.gsrSchemaDescription != null)
                    consumerProps.setProperty(AWSSchemaRegistryConstants.DESCRIPTION, KafkaClickstreamConsumer.gsrSchemaDescription);
                if (KafkaClickstreamConsumer.secondaryDeserializer)
                    consumerProps.setProperty(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER, KafkaAvroDeserializer.class.getName());
            } else {
                consumerProps.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, loadProps.getProperty("VALUE_DESERIALIZER_CLASS_CONFIG", VALUE_DESERIALIZER_CLASS_CONFIG).equals("") ? VALUE_DESERIALIZER_CLASS_CONFIG : loadProps.getProperty("VALUE_DESERIALIZER_CLASS_CONFIG", VALUE_DESERIALIZER_CLASS_CONFIG));
                consumerProps.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, loadProps.getProperty("KEY_DESERIALIZER_CLASS_CONFIG", KEY_DESERIALIZER_CLASS_CONFIG).equals("") ? KEY_DESERIALIZER_CLASS_CONFIG : loadProps.getProperty("KEY_SERIALIZER_CLASS_CONFIG", KEY_DESERIALIZER_CLASS_CONFIG));
            }
            consumerProps.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, loadProps.getProperty("SCHEMA_REGISTRY_URL_CONFIG", SCHEMA_REGISTRY_URL_CONFIG).equals("") ? SCHEMA_REGISTRY_URL_CONFIG : loadProps.getProperty("SCHEMA_REGISTRY_URL_CONFIG", SCHEMA_REGISTRY_URL_CONFIG));
            consumerProps.setProperty(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        }
        return consumerProps;
    }


}
