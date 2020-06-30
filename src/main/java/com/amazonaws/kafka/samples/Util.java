package com.amazonaws.kafka.samples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.RemoteClusterUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

class Util {
    private static final Logger logger = LogManager.getLogger(Util.class);

    static void writeFile(String fileLocation, String value, boolean append) throws IOException {
        try (BufferedWriter eventWriter = new BufferedWriter(new FileWriter(fileLocation, append))){
            eventWriter.write(value);
        } catch (FileNotFoundException e) {
            logger.error(String.format("Nonexistent path %s provided for file path. \n", fileLocation));
            throw e;
        }
    }

    private static Map<TopicPartition, OffsetAndMetadata> getTopicPartitionAndOffsets(String line, String delimiter2, String delimiter3){
        String[] topicPartitionOffsets = line.split(delimiter2);
        String[] topicPartition = topicPartitionOffsets[0].split(delimiter3);
        return Collections.singletonMap(new TopicPartition(topicPartition[0], Integer.parseInt(topicPartition[1])), new OffsetAndMetadata(Long.parseLong(topicPartitionOffsets[1])));
    }

    static Map<TopicPartition, OffsetAndMetadata> getOffsetsFromFile(String fileLocation, String word, String delimiter1, String delimiter2, String delimiter3) throws IOException {
        Map<TopicPartition, OffsetAndMetadata> lastOffsets = new HashMap<>();
        try(Stream<String> lines = Files.lines(Paths.get(fileLocation))){
            lines.filter(line -> line.contains(word)).forEach(
                    line -> lastOffsets.put(getTopicPartitionAndOffsets(line.split(delimiter1)[1], delimiter2, delimiter3).keySet().stream().findFirst().orElse(null), getTopicPartitionAndOffsets(line.split(delimiter1)[1], delimiter2, delimiter3).values().stream().findFirst().orElse(null))
            );
        }
        return lastOffsets;
    }

    static Map<TopicPartition, Long> getCheckpointLag(Map<TopicPartition, OffsetAndMetadata> OffsetsFromFile, Map<TopicPartition, OffsetAndMetadata> sourceCheckpointOffsets, MirrorClientSource mirrorClientSource) {
        logger.info("Getting Checkpoint lag between {} and {} \n", OffsetsFromFile, sourceCheckpointOffsets);
        Map<TopicPartition, Long> checkpointLag = new HashMap<>();
        Set<String> topics = new HashSet<>();
        sourceCheckpointOffsets.forEach((topicPartition, offsetAndMetadata) -> topics.add(topicPartition.topic()));

        OffsetsFromFile.forEach((topicPartition, offsetAndMetadata) -> {
            String replicatedTopic = mirrorClientSource.replicationPolicy().formatRemoteTopic(KafkaClickstreamConsumer.sourceCluster, topicPartition.topic());
            if (!topics.contains(replicatedTopic)) {
                logger.error("Topic in {} file not in the expected format. Expecting {} but got {} \n", KafkaClickstreamConsumer.bookmarkFileLocation, mirrorClientSource.replicationPolicy().originalTopic(replicatedTopic), topicPartition.topic());
                throw new RuntimeException(String.format("Topic in %s file not in the expected format. Expecting %s but got %s \n", KafkaClickstreamConsumer.bookmarkFileLocation, mirrorClientSource.replicationPolicy().originalTopic(replicatedTopic), topicPartition.topic()));
            }
            checkpointLag.put(topicPartition, offsetAndMetadata.offset() - sourceCheckpointOffsets.get(new TopicPartition(replicatedTopic, topicPartition.partition())).offset());
        });

        return checkpointLag;
    }

    static Map<TopicPartition, OffsetAndMetadata> getTranslatedOffsets(MM2Config mm2Config){
        final Map<TopicPartition,OffsetAndMetadata> mm2TranslatedOffsets;
        try {
            mm2TranslatedOffsets = new HashMap<>(RemoteClusterUtils.translateOffsets(mm2Config.mm2config(), (String) mm2Config.mm2config().get("source.cluster.alias"), ConsumerConfigs.consumerConfig().getProperty(ConsumerConfig.GROUP_ID_CONFIG), Duration.ofSeconds(20L)));
            logger.info("{} - Translated Offsets: {} \n%s", Thread.currentThread().getName(), mm2TranslatedOffsets);

        } catch ( InterruptedException | TimeoutException e) {
            logger.error(Util.stackTrace(e));
            throw new RuntimeException(String.format("Exception getting translated offsets. Cannot proceed. \n %s \n", e));
        }

        return mm2TranslatedOffsets;
    }

    static Map<TopicPartition, OffsetAndMetadata> updateMM2translatedOffsetsIfCheckpointLag(Map<TopicPartition, Long> checkpointLag, MirrorClientSource mirrorClientSource, Map<TopicPartition, OffsetAndMetadata> mm2TranslatedOffsets){
        checkpointLag.forEach((topicPartion, offset) -> {
            String replicatedTopic = mirrorClientSource.replicationPolicy().formatRemoteTopic(KafkaClickstreamConsumer.sourceCluster, topicPartion.topic());
            TopicPartition replicatedTopicPartition = new TopicPartition(replicatedTopic, topicPartion.partition());
            if (mm2TranslatedOffsets.containsKey(replicatedTopicPartition)){
                if (offset == 0L) {
                    logger.info ("{} - Replication checkpoint caught up for partition {} \n", Thread.currentThread().getName(), topicPartion.partition());
                } else
                {
                    if (offset > 0) {
                        logger.info ("{} - Replication checkpoint not caught up for partition {} \n", Thread.currentThread().getName(), topicPartion.partition());
                        logger.info("{} - There could be message duplication. Skipping ahead by {} messages from translated offset. \n", Thread.currentThread().getName(), offset);
                        mm2TranslatedOffsets.replace(replicatedTopicPartition, new OffsetAndMetadata(mm2TranslatedOffsets.get(replicatedTopicPartition).offset() + offset, null));
                    } else {
                        logger.info ("{} - Replication checkpoint behind offsets for partition {}. Not updating translated offsets. \n", Thread.currentThread().getName(), topicPartion.partition());
                    }
                }
            }

        });

        logger.info("{} - Final Translated Offsets: \n {}", Thread.currentThread().getName(), mm2TranslatedOffsets);
        return mm2TranslatedOffsets;
    }

    static String stackTrace(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

}
