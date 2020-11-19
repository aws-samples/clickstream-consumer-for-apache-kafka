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
    private Map<TopicPartition, OffsetAndMetadata> mm2TranslatedOffsets;

    private Map<TopicPartition, Long> checkpointLag;

    RunConsumer(Map<TopicPartition, OffsetAndMetadata> mm2TranslatedOffsets, String replicatedTopic) {
       this.mm2TranslatedOffsets = mm2TranslatedOffsets;
       consumer = new KafkaConsumerFactory().createConsumer();
       this.replicatedTopic = replicatedTopic;
    }

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

            consumer.subscribe(Pattern.compile(topicPattern), new Rebalance(consumer, currentOffsets, mm2TranslatedOffsets));
            consumer.poll(Duration.ofSeconds(2));

            while (!(cancel)) {
                ConsumerRecords<String, ClickEvent> records = consumer.poll(Duration.ofSeconds(10));
                if (records.count() == 0)
                    logger.info(Thread.currentThread().getName() + " - " + System.currentTimeMillis() + "  Waiting for data. Number of records retrieved: " + records.count());
                for (ConsumerRecord<String, ClickEvent> record : records) {
                    if (numberOfMessages == 0)
                        logger.info("{} - Topic = {}, Partition = {}, offset = {}, key = {}, value = {}\n", Thread.currentThread().getName(), record.topic(), record.partition(), record.offset(), record.key().trim(), record.value());
                    globalSeqNo = record.value().getGlobalseq();
                    propagationDelay += System.currentTimeMillis() - record.value().getEventtimestamp();
                    numberOfMessages++;
                    if (numberOfMessages % 1000 == 0){
                        avgPropagationDelay = (double)propagationDelay/numberOfMessages;
                        logger.info("{} - Avg Propagation delay in milliseconds: {} \n", Thread.currentThread().getName(), avgPropagationDelay);
                        logger.info("{} - Messages processed: {} \n", Thread.currentThread().getName(), numberOfMessages);
                        logger.info("{} - Topic = {}, Partition = {}, offset = {}, key = {}, value = {} \n", Thread.currentThread().getName(), record.topic(), record.partition(), record.offset(), record.key().trim(), record.value());
                    }

                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "No Metadata"));
                }
                consumer.commitAsync(currentOffsets, null);
            }
        } catch (WakeupException e) {
            // ignore for shutdown
            logger.info("{} - Consumer woken up\n", Thread.currentThread().getName());
        } catch (Exception e) {
            logger.error(Util.stackTrace(e));
        } finally {

            logger.info("{} - Last GlobalSeqNo = {} \n", Thread.currentThread().getName(), globalSeqNo);
            logger.info("{} - Last Offsets = {} \n", Thread.currentThread().getName(), currentOffsets);
            logger.info("{} - Messages processed = {} \n", Thread.currentThread().getName(), numberOfMessages);
            logger.info("{} - Avg Propagation delay in milliseconds: = {} \n", Thread.currentThread().getName(), avgPropagationDelay);
            Util.writeFile(KafkaClickstreamConsumer.bookmarkFileLocation, String.format("Last GlobalSeqNo:%d\n", globalSeqNo), true);
            Util.writeFile(KafkaClickstreamConsumer.bookmarkFileLocation, String.format("Messages Processed:%d\n", numberOfMessages), true);

            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : currentOffsets.entrySet()) {
                Util.writeFile(KafkaClickstreamConsumer.bookmarkFileLocation, String.format("TopicPartitionOffset:%s,%s\n", entry.getKey().toString(), entry.getValue().offset()), true);
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
