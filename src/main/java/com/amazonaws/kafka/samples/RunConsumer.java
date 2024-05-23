package com.amazonaws.kafka.samples;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import samples.clickstream.avro.ClickEvent;
import java.io.*;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

class RunConsumer implements Callable<String> {

    private static final Logger logger = LogManager.getLogger(RunConsumer.class);
    private boolean cancel = false;
    private final Consumer<String, ClickEvent> consumer;
    private final String replicatedTopic;

    RunConsumer(String replicatedTopic) {
        consumer = new KafkaConsumerFactory().createConsumer();
        this.replicatedTopic = replicatedTopic;
    }

    void shutdown() {
        cancel = true;
        consumer.wakeup();
    }

    @Override
    public String call() throws IOException {
        double avgPropagationDelay = 0;
        int numberOfMessages = 0;
        long propagationDelay = 0L;
        long globalSeqNo = 0L;
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        final String topicPattern = "(" + Pattern.quote(KafkaClickstreamConsumer.topic) + "|" + Pattern.quote((replicatedTopic)) + ")";

        try {

            consumer.subscribe(Pattern.compile(topicPattern), new Rebalance(consumer, currentOffsets));
            while (!(cancel)) {
                ConsumerRecords<String, ClickEvent> records = consumer.poll(Duration.ofSeconds(10));
                ConsumerRecord<String, ClickEvent>  lastRecord = null;
                for (ConsumerRecord<String, ClickEvent> record : records) {
                    globalSeqNo = record.value().getGlobalseq();
                    propagationDelay += System.currentTimeMillis() - record.value().getEventtimestamp();
                    numberOfMessages++;
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "No Metadata"));
                    lastRecord = record;
                }
                if (lastRecord == null)
                    continue;
                    
                if (numberOfMessages % 100_000 == 0) {
                    avgPropagationDelay = (double)propagationDelay/numberOfMessages;
                    logger.info("{} - Messages processed: {} \n", Thread.currentThread().getName(), numberOfMessages);
                    logger.info("{} - Avg Propagation delay in milliseconds: {} \n", Thread.currentThread().getName(), avgPropagationDelay);
                    logger.info("{} - Current Offsets: {} \n", Thread.currentThread().getName(), currentOffsets);
                    logger.info("{} - Last record - Topic = {}, Partition = {}, offset = {}, key = {}, value = {} \n", Thread.currentThread().getName(), lastRecord.topic(), lastRecord.partition(), lastRecord.offset(), lastRecord.key().trim(), lastRecord.value());
                }
                long start = System.currentTimeMillis();
                logger.debug("Beginning commitSync, start time: {}", start);
                consumer.commitSync(currentOffsets);
                logger.debug("Finished commitSync, total time: {} millis", System.currentTimeMillis() - start);
            }
        } catch (WakeupException e) {
            // ignore for shutdown
            logger.info("{} - Consumer woken up\n", Thread.currentThread().getName());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {

            logger.info("{} - Last GlobalSeqNo = {} \n", Thread.currentThread().getName(), globalSeqNo);
            logger.info("{} - Last Offsets = {} \n", Thread.currentThread().getName(), currentOffsets);
            logger.info("{} - Messages processed = {} \n", Thread.currentThread().getName(), numberOfMessages);
            logger.info("{} - Avg Propagation delay in milliseconds: = {} \n", Thread.currentThread().getName(), avgPropagationDelay);
            logger.info("Last GlobalSeqNo: {}\n", globalSeqNo);
            logger.info("Messages Processed: {}\n", numberOfMessages);

            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : currentOffsets.entrySet()) {
                logger.info("TopicPartitionOffset: {},{}\n", entry.getKey().toString(), entry.getValue().offset());
            }

            try {
                logger.info("{} - Doing final commit\n", Thread.currentThread().getName());
                consumer.commitSync(currentOffsets);
            } finally {
                consumer.close();
                logger.info("{} - Closed consumer\n", Thread.currentThread().getName());
            }

        }
        return "Task executed in " + Thread.currentThread().getName();
    }
}
