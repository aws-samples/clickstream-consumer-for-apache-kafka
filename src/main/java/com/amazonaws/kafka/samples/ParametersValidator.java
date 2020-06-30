package com.amazonaws.kafka.samples;

import com.beust.jcommander.ParameterException;

public class ParametersValidator{

    static void validate() throws ParameterException{
        if (KafkaClickstreamConsumer.failover && (KafkaClickstreamConsumer.sourceCluster == null || KafkaClickstreamConsumer.destCluster == null)){
            throw new ParameterException("If parameter --failover (or -flo) is specified, the parameters --sourceCluster (or -src) and --destCluster (or -dst) also need to be specified.");
        }
    }
}
