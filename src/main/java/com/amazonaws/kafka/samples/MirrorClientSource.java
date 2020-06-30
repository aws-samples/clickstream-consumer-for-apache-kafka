package com.amazonaws.kafka.samples;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.mirror.Checkpoint;
import org.apache.kafka.connect.mirror.MirrorClient;
import org.apache.kafka.connect.mirror.MirrorClientConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

class MirrorClientSource extends MirrorClient {

    static final String CHECKPOINTS_TOPIC_SUFFIX = ".checkpoints.internal";
    private static final Logger logger = LogManager.getLogger(MirrorClientSource.class);

    public MirrorClientSource(Map<String, Object> props) {
        super(props);
    }

    public MirrorClientSource(MirrorClientConfig config) {
        super(config);
    }

    Map<TopicPartition, OffsetAndMetadata> sourceConsumerOffsets(String consumerGroupId,
                                                                        String remoteClusterAlias, Duration timeout) throws IOException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(ConsumerConfigs.consumerConfig(),
                new ByteArrayDeserializer(), new ByteArrayDeserializer());
        try {
            String checkpointTopic = remoteClusterAlias + CHECKPOINTS_TOPIC_SUFFIX;
            List<TopicPartition> checkpointAssignment =
                    Collections.singletonList(new TopicPartition(checkpointTopic, 0));
            consumer.assign(checkpointAssignment);
            consumer.seekToBeginning(checkpointAssignment);
            while (System.currentTimeMillis() < deadline && !endOfStream(consumer, checkpointAssignment)) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    try {
                        Checkpoint checkpoint = Checkpoint.deserializeRecord(record);
                        if (checkpoint.consumerGroupId().equals(consumerGroupId)) {
                            offsets.put(checkpoint.topicPartition(), new OffsetAndMetadata(checkpoint.upstreamOffset(), null));
                        }
                    } catch (SchemaException e) {
                        logger.info("Could not deserialize record. Skipping.", e);
                    }
                }
            }
            logger.info("Consumed {} checkpoint records for {} from {}.", offsets.size(),
                    consumerGroupId, checkpointTopic);
        } finally {
            consumer.close();
        }
        return offsets;
    }

    static private boolean endOfStream(Consumer<?, ?> consumer, Collection<TopicPartition> assignments) {
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignments);
        for (TopicPartition topicPartition : assignments) {
            if (consumer.position(topicPartition) < endOffsets.get(topicPartition)) {
                return false;
            }
        }
        return true;
    }

}
