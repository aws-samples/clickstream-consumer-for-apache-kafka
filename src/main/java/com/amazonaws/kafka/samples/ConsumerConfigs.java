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
        mTLSEnable = KafkaClickstreamConsumer.mTLSEnable;
        sslEnable = mTLSEnable || KafkaClickstreamConsumer.sslEnable;
        saslScramEnable = KafkaClickstreamConsumer.saslscramEnable;
        iamEnable = KafkaClickstreamConsumer.iamEnable;
        glueSchemaRegistry = KafkaClickstreamConsumer.glueSchemaRegistry;

        Properties consumerProps = new Properties();
        try (FileInputStream file = new FileInputStream(KafkaClickstreamConsumer.propertiesFilePath)) {
            consumerProps.load(file);
        } 
        catch(IOException e) {
            logger.error("Properties file path not found in location: {}\n", KafkaClickstreamConsumer.propertiesFilePath);
        }

        consumerProps.setProperty(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        if (sslEnable) {
            consumerProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
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
        }
        logger.info("Consumer Properties: \n{}", consumerProps);
        return consumerProps;
    }


}
