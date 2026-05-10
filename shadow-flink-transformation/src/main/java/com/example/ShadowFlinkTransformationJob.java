package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;


import java.util.Properties;

// --- Transaction POJO ---######
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

public class ShadowFlinkTransformationJob {

    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

        ObjectMapper mapper = new ObjectMapper();

        // ✅ Kafka config (IMPORTANT: cluster DNS, not localhost)
        String brokers = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS",
                "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
        );

        String groupId = System.getenv().getOrDefault(
                "GROUP_ID",
                "flink-consumer-v1"
        );

        //Properties props = new Properties();
        //props.setProperty("bootstrap.servers", brokers);
        //props.setProperty("group.id", groupId);
        //props.setProperty("auto.offset.reset", "earliest");

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", brokers);
        consumerProps.setProperty("group.id", groupId);
        consumerProps.setProperty("auto.offset.reset", "earliest");

        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", brokers);
        
        


        // --- SOURCE ---###
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>(
                        "input-topic",
                        new SimpleStringSchema(),
                        consumerProps
                );

        

        //consumer.setStartFromGroupOffsets();

        consumer.setStartFromEarliest();

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
                                tx.currency.replace("Hello ", "ShadowTest");

                        return mapper.writeValueAsString(enriched);

                    } catch (Exception e) {
                        e.printStackTrace();
                        return null;
                    }
                })
                .filter(value -> value != null);

        // --- SINK ---#########
        FlinkKafkaProducer<String> producer =
                new FlinkKafkaProducer<>(
                        "shadow-output-topic",
                        new SimpleStringSchema(),
                        producerProps
                        //Semantic.AT_LEAST_ONCE   // ✅ important
                );

        stream.addSink(producer);

        env.execute("Flink Transaction Enrichment Job");
    }
}