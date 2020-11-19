package com.amazonaws.kafka.samples;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.connect.mirror.MirrorClientConfig;
import java.util.HashMap;
import java.util.Map;

class MM2Config {

    private boolean sslEnable = false;
    private boolean mTLSEnable = false;
    private static boolean saslScramEnable = false;

    Map<String, Object> mm2config() {

        Map<String, Object> mm2Props = new HashMap<>();
        mm2Props.put("bootstrap.servers", KafkaClickstreamConsumer.consumerProperties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        mm2Props.put("source.cluster.alias", KafkaClickstreamConsumer.sourceCluster);

        if (KafkaClickstreamConsumer.mTLSEnable){
            mTLSEnable = true;
            sslEnable = true;
        } else {
            sslEnable = KafkaClickstreamConsumer.sslEnable;
        }

        if (KafkaClickstreamConsumer.saslscramEnable) {
            saslScramEnable = true;
        }

        if (sslEnable){
            mm2Props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            mm2Props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, ConsumerConfigs.consumerConfig().getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        }
        if (mTLSEnable){
            mm2Props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, KafkaClickstreamConsumer.consumerProperties.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
            mm2Props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, KafkaClickstreamConsumer.consumerProperties.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
            mm2Props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, KafkaClickstreamConsumer.consumerProperties.getProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG));
        }
        if (saslScramEnable) {
            mm2Props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, KafkaClickstreamConsumer.consumerProperties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
            mm2Props.put(SaslConfigs.SASL_MECHANISM, KafkaClickstreamConsumer.consumerProperties.getProperty(SaslConfigs.SASL_MECHANISM));
            mm2Props.put(SaslConfigs.SASL_JAAS_CONFIG, KafkaClickstreamConsumer.consumerProperties.getProperty(SaslConfigs.SASL_JAAS_CONFIG));
        }
        if (!KafkaClickstreamConsumer.replicationPolicyClass.equalsIgnoreCase(String.valueOf(MirrorClientConfig.REPLICATION_POLICY_CLASS_DEFAULT))){
            mm2Props.put(MirrorClientConfig.REPLICATION_POLICY_CLASS, KafkaClickstreamConsumer.replicationPolicyClass);
        }

        if (!KafkaClickstreamConsumer.replicationPolicySeparator.equalsIgnoreCase(MirrorClientConfig.REPLICATION_POLICY_SEPARATOR_DEFAULT)){
            mm2Props.put(MirrorClientConfig.REPLICATION_POLICY_SEPARATOR, KafkaClickstreamConsumer.replicationPolicySeparator);
        }

        return mm2Props;
    }
}
