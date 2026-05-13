package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

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
                "flink-consumer-v1"
        );

        // ====================================================
        // OUTPUT TOPIC ENV VARIABLE
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
        System.out.println("=================================");

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
                "earliest"
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

        consumer.setStartFromEarliest();

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
        // FEATURE FLAG CONTROL
        // ====================================================

        if (enabled) {

            System.out.println(
                    "Feature flag enabled. " +
                    "Sinking to Kafka topic: " +
                    outputTopic
            );

            // ================================================
            // SINK
            // ================================================

            FlinkKafkaProducer<String> producer =
                    new FlinkKafkaProducer<>(
                            outputTopic,
                            new SimpleStringSchema(),
                            producerProps
                    );

            stream.addSink(producer);

        } else {

            System.out.println(
                    "feature flag disabled"
            );
        }

        // ====================================================
        // EXECUTE JOB
        // ====================================================

        env.execute(
                "Flink Transaction Enrichment Job"
        );
    }
}