package com.amazonaws.kafka.samples;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import samples.clickstream.avro.ClickEvent;
import java.util.Collection;
import java.util.Map;

class Rebalance implements ConsumerRebalanceListener {

    private Consumer<String, ClickEvent> consumer;
    private int partitionsRevokedCounter = 0;
    private int partitionsAssignedCounter = 0;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets;
    private static final Logger logger = LogManager.getLogger(Rebalance.class);

    Rebalance(Consumer<String, ClickEvent> consumer, Map<TopicPartition, OffsetAndMetadata> currentOffsets){
        this.consumer = consumer;
        this.currentOffsets = currentOffsets;
    }
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("{} - Partitions revoked: {} \n", Thread.currentThread().getName(), partitions);
        partitionsRevokedCounter++;
        logger.info("{} - Rebalance Triggered. Rebalance count: {} \n", Thread.currentThread().getName(), partitionsRevokedCounter);
        logger.info("{} - Committing offsets - {} \n", Thread.currentThread().getName(), currentOffsets);
        consumer.commitSync(currentOffsets);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.info("{} - Consumer assignment: {} \n", Thread.currentThread().getName(), partitions);
        partitions.forEach(i -> logger.info("{} - Consumer position: {} - {} \n", Thread.currentThread().getName(), i, consumer.position(i)));
        partitionsAssignedCounter++;
    }
}
