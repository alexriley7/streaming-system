package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

// --- Transaction POJO ---
class Transaction {
    public String userId;
    public String transactionId;
    public double amount;
    public String currency;
    public long timestamp;
}

// --- Enriched Transaction ---
class EnrichedTransaction extends Transaction {
    public boolean highValue;
    public String normalizedCurrency;
}

public class FlinkTransformationJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        ObjectMapper mapper = new ObjectMapper();

        // Kafka config
        Properties props = new Properties();
        props.setProperty(
                "bootstrap.servers",
                System.getenv().getOrDefault(
                        "KAFKA_BOOTSTRAP_SERVERS",
                        "localhost:9092"
                )
        );

        props.setProperty(
                "group.id",
                System.getenv().getOrDefault(
                        "GROUP_ID",
                        "flink-consumer-v1"
                )
        );

        // --- SOURCE ---
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>(
                        "input-topic",
                        new SimpleStringSchema(),
                        props
                );

        consumer.setStartFromLatest();

        // --- TRANSFORMATION ---
        var stream = env
                .addSource(consumer)
                .map(value -> {
                    try {
                        Transaction tx = mapper.readValue(value, Transaction.class);

                        EnrichedTransaction enriched = new EnrichedTransaction();

                        // copy fields
                        enriched.userId = tx.userId;
                        enriched.transactionId = tx.transactionId;
                        enriched.amount = tx.amount;
                        enriched.currency = tx.currency;
                        enriched.timestamp = tx.timestamp;

                        // 🔥 enrichment logic
                        enriched.highValue = tx.amount > 500;
                        enriched.normalizedCurrency =
                                tx.currency.replace("Hello ", "");

                        return mapper.writeValueAsString(enriched);

                    } catch (Exception e) {
                        e.printStackTrace();
                        return null;
                    }
                })
                .filter(value -> value != null);

        // --- SINK ---###
        FlinkKafkaProducer<String> producer =
                new FlinkKafkaProducer<>(
                        "output-topic-v1",
                        new SimpleStringSchema(),
                        props
                );

        stream.addSink(producer);

        env.execute("Flink Transaction Enrichment Job");
    }
}