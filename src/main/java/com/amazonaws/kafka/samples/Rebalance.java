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
    private Map<TopicPartition, OffsetAndMetadata> mm2TranslatedOffsets;
    private static final Logger logger = LogManager.getLogger(Rebalance.class);

    Rebalance(Consumer<String, ClickEvent> consumer, Map<TopicPartition, OffsetAndMetadata> currentOffsets, Map<TopicPartition, OffsetAndMetadata> mm2TranslatedOffsets){
        this.consumer = consumer;
        this.currentOffsets = currentOffsets;
        this.mm2TranslatedOffsets = mm2TranslatedOffsets;
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
        if (partitionsAssignedCounter == 0) {
            if (KafkaClickstreamConsumer.failover){

                logger.info("{} - Consumer assignment: {} \n", Thread.currentThread().getName(), partitions);
                partitions.forEach(i -> logger.info("{} - Consumer position: {} - {} \n", Thread.currentThread().getName(), i, consumer.position(i)));

                for (TopicPartition topicPartition: partitions){
                    if (mm2TranslatedOffsets.containsKey(topicPartition)){
                        logger.info("{} - Snapping consumer to the translated offset {} for TopicPartition {} \n", Thread.currentThread().getName(), mm2TranslatedOffsets.get(topicPartition).offset(), topicPartition);
                        consumer.seek(topicPartition, mm2TranslatedOffsets.get(topicPartition));
                    }
                }

            } else {
                if (KafkaClickstreamConsumer.sourceRewind){
                    logger.info("{} - Consumer assignment: {} \n", Thread.currentThread().getName(), partitions);
                    partitions.forEach(i -> logger.info("{} - Consumer position: {} - {} \n", Thread.currentThread().getName(), i, consumer.position(i)));

                    logger.info("{} - Consumer rewind specified. Rewinding consumer to the beginning \n", Thread.currentThread().getName());
                    consumer.seekToBeginning(partitions);

                    logger.info("{} - Consumer assignment: {} \n", Thread.currentThread().getName(), partitions);
                    partitions.forEach(i -> logger.info("{} - Consumer position: {} - {} \n", Thread.currentThread().getName(), i, consumer.position(i)));
                } else {
                    logger.info("{} - Consumer assignment: {} \n", Thread.currentThread().getName(), partitions);
                    partitions.forEach(i -> logger.info("{} - Consumer position: {} - {} \n", Thread.currentThread().getName(), i, consumer.position(i)));
                }
            }
        } else {
            logger.info("{} - Consumer assignment: {} \n", Thread.currentThread().getName(), partitions);
            partitions.forEach(i -> logger.info("{} - Consumer position: {} - {} \n", Thread.currentThread().getName(), i, consumer.position(i)));
        }
        partitionsAssignedCounter++;
    }
}
