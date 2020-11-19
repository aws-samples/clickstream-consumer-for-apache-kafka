package com.amazonaws.kafka.samples;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import samples.clickstream.avro.ClickEvent;

class KafkaConsumerFactory {
    Consumer<String, ClickEvent> createConsumer() {
        return new KafkaConsumer<>(KafkaClickstreamConsumer.consumerProperties);
    }
}
