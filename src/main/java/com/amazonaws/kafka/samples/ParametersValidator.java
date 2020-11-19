package com.amazonaws.kafka.samples;

import com.beust.jcommander.ParameterException;

class ParametersValidator{

    static void validate() throws ParameterException{
        if (KafkaClickstreamConsumer.failover && (KafkaClickstreamConsumer.sourceCluster == null || KafkaClickstreamConsumer.destCluster == null)){
            throw new ParameterException("If parameter --failover (or -flo) is specified, the parameters --sourceCluster (or -src) and --destCluster (or -dst) also need to be specified.");
        }
        if (KafkaClickstreamConsumer.saslscramEnable && KafkaClickstreamConsumer.mTLSEnable) {
            throw new ParameterException("Specify either --mTLSEnable (or -mtls) or --saslscramEnable (or -sse). Not both.");
        }
        if (KafkaClickstreamConsumer.saslscramEnable && (KafkaClickstreamConsumer.saslscramUser == null || KafkaClickstreamConsumer.saslscramUser.equalsIgnoreCase(""))) {
            throw new ParameterException("If parameter --saslscramEnable (or -sse) is specified, the parameter --saslscramUser (or -ssu) needs to be specified.");
        }
        if (!KafkaClickstreamConsumer.saslscramEnable && KafkaClickstreamConsumer.saslscramUser != null) {
            throw new ParameterException("If parameter --saslscramUser (or -ssu) is specified, the parameter --saslscramEnable (or -sse) needs to be specified.");
        }
        if (KafkaClickstreamConsumer.saslscramEnable && KafkaClickstreamConsumer.sslEnable) {
            throw new ParameterException("Specify either --sslEnable (or -ssl) or --saslscramEnable (or -sse). Not both.");
        }
    }
}
