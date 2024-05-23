package com.amazonaws.kafka.samples;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
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
    private static Integer numThreads = 1;

    @Parameter(names = {"--runFor", "-rf"})
    private static Integer runFor = 0;

    @Parameter(names = {"--sslEnable", "-ssl"})
    static boolean sslEnable = false;

    @Parameter(names = {"--mTLSEnable", "-mtls"})
    static boolean mTLSEnable = false;

    @Parameter(names = {"--saslscramEnable", "-sse"})
    static boolean saslscramEnable = false;

    @Parameter(names = {"--iamEnable", "-iam"})
    static boolean iamEnable = false;

    @Parameter(names = {"--saslscramUser", "-ssu"})
    static String saslscramUser;

    @Parameter(names = {"--glueSchemaRegistry", "-gsr"})
    static boolean glueSchemaRegistry = false;

    @Parameter(names = {"--failover", "-flo"})
    static boolean failover = false;

    @Parameter(names = {"--sourceCluster", "-src"})
    static String sourceCluster = "msksource";

    @Parameter(names = {"--gsrRegistryName", "-grn"})
    static String gsrRegistryName;

    @Parameter(names = {"--gsrSchemaName", "-gsn"})
    static String gsrSchemaName;

    @Parameter(names = {"--gsrSchemaDescription", "-gsd"})
    static String gsrSchemaDescription;

    @Parameter(names = {"--secondaryDeserializer", "-sdd"})
    static boolean secondaryDeserializer = false;

    @Parameter(names = {"--destCluster", "-dst"})
    static String destCluster = "mskdest";

    @Parameter(names = {"--region", "-reg"})
    static String region = "us-east-1";

    @Parameter(names = {"--gsrRegion", "-gsrr"})
    static String gsrRegion = "us-east-1";

    static Properties consumerProperties;

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
            logger.error(e.getMessage(), e);
        }

        try {
            executor.shutdown();
            while (!executor.awaitTermination(5L, TimeUnit.SECONDS)) {
                logger.info("Not yet. Still waiting for consumer(s) termination");
            }
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        long endTime = System.nanoTime();
        logger.info("End Timestamp: {} \n", TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
        long executionTime = endTime - startTime;
        logger.info("Execution time in milliseconds: {} \n", TimeUnit.NANOSECONDS.toMillis(executionTime));
    }

    public static void main(String[] args) throws IOException {

        startTime = System.nanoTime();
        logger.info("Start time: {} \n", TimeUnit.NANOSECONDS.toMillis(startTime));
        final KafkaClickstreamConsumer kafkaClickstreamConsumer = new KafkaClickstreamConsumer();

        JCommander jc = JCommander.newBuilder()
                .addObject(kafkaClickstreamConsumer)
                .build();
        jc.parse(args);
        if (kafkaClickstreamConsumer.help) {
            jc.usage();
            return;
        }
        ParametersValidator.validate();
        consumerProperties = ConsumerConfigs.consumerConfig();

        List<RunConsumer> executeTasks = new ArrayList<>();
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        // Registering a shutdown hook so we can exit cleanly
        Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaClickstreamConsumer.shutdown(executeTasks, executor)));

        for (Integer i = 0; i < numThreads; i++) {
            executeTasks.add(new RunConsumer(topic));
        }

        Collection<Future<String>> futures = submitAll(executor, executeTasks);
        String result;

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }

        for (Future<String> future : futures) {
            try {
                logger.trace("Future status: {} ", future.isDone());
                if (future.isDone()) {
                    result = future.get();
                    logger.info(result);
                }
            } catch (InterruptedException | ExecutionException e) {
                logger.error(e.getMessage(), e);
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
                    logger.error(e.getMessage(), e);
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
                logger.error(e.getMessage(), e);
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
