package com.amazonaws.kafka.samples;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.MirrorClientConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;


public class KafkaClickstreamConsumer {

    private static final Logger logger = LogManager.getLogger(KafkaClickstreamConsumer.class);
    private static long startTime;

    @Parameter(names = {"--help", "-h"}, help = true)
    private boolean help = false;

    @Parameter(names = {"--topic", "-t"})
    static String topic = "ExampleTopic";

    @Parameter(names = {"--propertiesFilePath", "-pfp"})
    static String propertiesFilePath = "/tmp/kafka/consumer.properties";

    @Parameter(names = {"--numThreads", "-nt"})
    private static Integer numThreads = 2;

    @Parameter(names = {"--runFor", "-rf"})
    private static Integer runFor = 0;

    @Parameter(names = {"--sslEnable", "-ssl"})
    static boolean sslEnable = false;

    @Parameter(names = {"--mTLSEnable", "-mtls"})
    static boolean mTLSEnable = false;

    @Parameter(names = {"--failover", "-flo"})
    static boolean failover = false;

    @Parameter(names = {"--sourceRewind", "-srr"})
    static boolean sourceRewind = false;

    @Parameter(names = {"--sourceCluster", "-src"})
    static String sourceCluster = "msksource";

    @Parameter(names = {"--destCluster", "-dst"})
    static String destCluster = "mskdest";

    @Parameter(names = {"--replicationPolicySeparator", "-rps"})
    static String replicationPolicySeparator = MirrorClientConfig.REPLICATION_POLICY_SEPARATOR_DEFAULT;

    @Parameter(names = {"--replicationPolicyClass", "-rpc"})
    static String replicationPolicyClass = String.valueOf(MirrorClientConfig.REPLICATION_POLICY_CLASS_DEFAULT);

    static String bookmarkFileLocation = "/tmp/consumer_bookmark.txt";
    private static final String failoverBookmarkFileLocation = "/tmp/consumer_bookmark.txt";

    private static <T> Collection<Future<T>> submitAll(ExecutorService service, Collection<? extends Callable<T>> tasks) {
        Collection<Future<T>> futures = new ArrayList<>(tasks.size());
        for (Callable<T> task : tasks) {
            futures.add(service.submit(task));
        }
        return futures;
    }

    private void shutdown(List<RunConsumer> executeTasks, ExecutorService executor) {
        logger.info("Starting exit...");

        for (RunConsumer consumer : executeTasks) {
            consumer.shutdown();
        }

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            logger.error(Util.stackTrace(e));
        }

        try {
            executor.shutdown();
            while (!executor.awaitTermination(5L, TimeUnit.SECONDS)) {
                logger.info("Not yet. Still waiting for consumer(s) termination");
            }
        } catch (InterruptedException e) {
            logger.error(Util.stackTrace(e));
        }
        long endTime = System.nanoTime();
        logger.info("End Timestamp: {} \n", TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
        long executionTime = endTime - startTime;
        logger.info("Execution time in milliseconds: {} \n", TimeUnit.NANOSECONDS.toMillis(executionTime));
    }

    private Map<TopicPartition, Long> getCheckPointLag(MirrorClientSource mirrorClientSource) throws IOException {

        Map<TopicPartition, OffsetAndMetadata> sourceConsumerOffsets = mirrorClientSource.sourceConsumerOffsets(ConsumerConfigs.consumerConfig().getProperty(ConsumerConfig.GROUP_ID_CONFIG), KafkaClickstreamConsumer.sourceCluster, Duration.ofSeconds(20L));
        if (sourceConsumerOffsets.size() == 0) {
            logger.error("Error retrieving source consumer offsets from the MM2 checkpoint topic.");
            throw new RuntimeException("Error retrieving source consumer offsets from the MM2 checkpoint topic.");
        }
        Map<TopicPartition, OffsetAndMetadata> offsetsFromFile = Util.getOffsetsFromFile(KafkaClickstreamConsumer.bookmarkFileLocation, "TopicPartitionOffset", ":", ",", "-");
        if (offsetsFromFile.size() == 0) {
            logger.error("Error retrieving source last consumer offsets from the consumer bookmark file {}. \n", bookmarkFileLocation);
            throw new RuntimeException("Error retrieving source consumer offsets from the MM2 checkpoint topic.");
        }
        final Map<TopicPartition, Long> checkpointLag = Util.getCheckpointLag(offsetsFromFile, sourceConsumerOffsets, mirrorClientSource);
        logger.info("Checkpoint Lag {} \n", checkpointLag);
        return checkpointLag;
    }


    public static void main(String[] args) throws IOException {

        startTime = System.nanoTime();
        logger.info("Start time: {} \n", TimeUnit.NANOSECONDS.toMillis(startTime));
        final KafkaClickstreamConsumer kafkaClickstreamConsumer = new KafkaClickstreamConsumer();
        Map<TopicPartition, OffsetAndMetadata> mm2TranslatedOffsets = null;
        Map<TopicPartition, Long> checkpointLag;

        JCommander jc = JCommander.newBuilder()
                .addObject(kafkaClickstreamConsumer)
                .build();
        jc.parse(args);
        if (kafkaClickstreamConsumer.help) {
            jc.usage();
            return;
        }
        ParametersValidator.validate();
        MM2Config mm2Config = new MM2Config();
        MirrorClientSource mirrorClientSource = new MirrorClientSource(mm2Config.mm2config());

        if (failover) {
            checkpointLag = kafkaClickstreamConsumer.getCheckPointLag(mirrorClientSource);
            mm2TranslatedOffsets = Util.updateMM2translatedOffsetsIfCheckpointLag(checkpointLag, mirrorClientSource, Util.getTranslatedOffsets(mm2Config));
            bookmarkFileLocation = failoverBookmarkFileLocation;
        }

        Util.writeFile(bookmarkFileLocation, "", false);


        List<RunConsumer> executeTasks = new ArrayList<>();
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);


        // Registering a shutdown hook so we can exit cleanly
        Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaClickstreamConsumer.shutdown(executeTasks, executor)));

        final String replicatedTopic = mirrorClientSource.replicationPolicy().formatRemoteTopic(KafkaClickstreamConsumer.sourceCluster, topic);

        for (Integer i = 0; i < numThreads; i++) {
            if (failover) {
                executeTasks.add(new RunConsumer(mm2TranslatedOffsets, replicatedTopic));
            } else {
                executeTasks.add(new RunConsumer(replicatedTopic));
            }
        }

        Collection<Future<String>> futures = submitAll(executor, executeTasks);
        String result;

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            logger.error(Util.stackTrace(e));
        }

        for (Future<String> future : futures) {
            try {
                logger.trace("Future status: {} ", future.isDone());
                if (future.isDone()) {
                    result = future.get();
                    logger.info(result);
                }
            } catch (InterruptedException | ExecutionException e) {
                logger.error(Util.stackTrace(e));
                System.exit(1);
            }
        }

        int tasksDone = 0;
        if (runFor > 0) {
            int runTime = 0;

            while (runTime < runFor) {
                try {
                    TimeUnit.SECONDS.sleep(2L);
                } catch (InterruptedException e) {
                    logger.error(Util.stackTrace(e));
                }
                for (Future<String> future : futures) {
                    if (future.isDone()) {
                        tasksDone += 1;
                        if (tasksDone == numThreads){
                            System.exit(0);
                        }

                    }
                }
                runTime += 2;
            }
            logger.info("Reached specified run time of {} seconds. Shutting down. \n", runFor);
            System.exit(0);
        }

        while (tasksDone < numThreads){
            try {
                TimeUnit.SECONDS.sleep(2L);
            } catch (InterruptedException e) {
                logger.error(Util.stackTrace(e));
            }
            for (Future<String> future : futures) {
                if (future.isDone()) {
                    tasksDone += 1;
                }
            }
        }

        System.exit(0);
    }

}
