package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.connector.file.sink.FileSink;
// import org.apache.flink.connector.file.sink.writer.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;

import org.apache.flink.core.fs.Path;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;


import java.time.Duration;
import java.util.Properties;


// ============================================================
// TRANSACTION POJO
// ============================================================

class Transaction {

    public String userId;
    public String transactionId;
    public double amount;
    public String currency;
    public long timestamp;
}


// ============================================================
// ENRICHED TRANSACTION
// ============================================================

class EnrichedTransaction extends Transaction {

    public boolean highValue;
    public String normalizedCurrency;
}


// ============================================================
// MAIN JOB
// ============================================================

public class ShadowFlinkTransformationJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

        ObjectMapper mapper = new ObjectMapper();

        // ====================================================
        // KAFKA CONFIG
        // ====================================================

        String brokers = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS",
                "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
        );

        String groupId = System.getenv().getOrDefault(
                "GROUP_ID",
                "flink-consumer-v2"
        );

        // ====================================================
        // OUTPUT TOPIC
        // ====================================================

        String outputTopic = System.getenv().getOrDefault(
                "OUTPUT_TOPIC",
                "shadow-output-topic"
        );

        // ====================================================
        // FEATURE FLAG
        // ====================================================

        boolean enabled = Boolean.parseBoolean(
                System.getenv()
                        .getOrDefault(
                                "ENABLE_JOB",
                                "true"
                        )
        );

        System.out.println("=================================");
        System.out.println("ENABLE_JOB = " + enabled);
        System.out.println("OUTPUT_TOPIC = " + outputTopic);
        System.out.println("GROUP_ID = " + groupId);
        System.out.println("=================================");

        // ====================================================
        // ENABLE CHECKPOINTING
        // ====================================================

        env.enableCheckpointing(30000);

        // ====================================================
        // CONSUMER PROPERTIES
        // ====================================================

        Properties consumerProps = new Properties();

        consumerProps.setProperty(
                "bootstrap.servers",
                brokers
        );

        consumerProps.setProperty(
                "group.id",
                groupId
        );

        consumerProps.setProperty(
                "auto.offset.reset",
                "latest"
        );

        // ====================================================
        // PRODUCER PROPERTIES
        // ====================================================

        Properties producerProps = new Properties();

        producerProps.setProperty(
                "bootstrap.servers",
                brokers
        );

        // ====================================================
        // SOURCE
        // ====================================================

        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>(
                        "input-topic",
                        new SimpleStringSchema(),
                        consumerProps
                );

        // IMPORTANT:
        // IGNORE OLD HISTORICAL MESSAGES

        consumer.setStartFromLatest();

        // ====================================================
        // TRANSFORMATION
        // ====================================================

        var stream = env
                .addSource(consumer)
                .map(value -> {

                    try {

                        Transaction tx =
                                mapper.readValue(
                                        value,
                                        Transaction.class
                                );

                        EnrichedTransaction enriched =
                                new EnrichedTransaction();

                        // ------------------------------------
                        // COPY FIELDS
                        // ------------------------------------

                        enriched.userId = tx.userId;
                        enriched.transactionId = tx.transactionId;
                        enriched.amount = tx.amount;
                        enriched.currency = tx.currency;
                        enriched.timestamp = tx.timestamp;

                        // ------------------------------------
                        // ENRICHMENT LOGIC
                        // ------------------------------------

                        enriched.highValue =
                                tx.amount > 500;

                        enriched.normalizedCurrency =
                                tx.currency.replace(
                                        "Hello ",
                                        "ShadowTestFinal"
                                );

                        // RETURN JSON STRING

                        return mapper.writeValueAsString(
                                enriched
                        );

                    } catch (Exception e) {

                        e.printStackTrace();

                        return null;
                    }
                })
                .filter(value -> value != null);

        // ====================================================
        // KAFKA SINK
        // ====================================================

        if (enabled) {

            System.out.println(
                    "Feature flag enabled. " +
                    "Sinking to Kafka topic: " +
                    outputTopic
            );

            FlinkKafkaProducer<String> producer =
                    new FlinkKafkaProducer<>(
                            outputTopic,
                            new SimpleStringSchema(),
                            producerProps
                    );

            stream.addSink(producer);

        } else {

            System.out.println(
                    "Feature flag disabled"
            );
        }

        // ====================================================
        // MOTO S3 JSON FILE SINK
        // ====================================================

        // IMPORTANT:
        // Requires Flink S3 filesystem plugin in image:
        //
        // flink-s3-fs-hadoop OR flink-s3-fs-presto
        //
        // and Hadoop AWS dependencies.

        String s3Path =
                "s3://shadow-flink-job/events/";

        FileSink<String> s3Sink =
                FileSink
                        .forRowFormat(
                                new Path(s3Path),
                                new SimpleStringEncoder<String>("UTF-8")
                        )
                        .withRollingPolicy(
                                DefaultRollingPolicy.builder()
                                        .withRolloverInterval(
                                                Duration.ofMinutes(1)
                                                        .toMillis()
                                        )
                                        .withInactivityInterval(
                                                Duration.ofSeconds(30)
                                                        .toMillis()
                                        )
                                        .withMaxPartSize(
                                                1024 * 1024 * 10
                                        )
                                        .build()
                        )
                        .build();

        stream.sinkTo(s3Sink);

        System.out.println(
                "S3 sink enabled -> " + s3Path
        );

        // ====================================================
        // EXECUTE JOB
        // ====================================================

        env.execute(
                "Shadow Flink Transaction Enrichment Job"
        );
    }
}