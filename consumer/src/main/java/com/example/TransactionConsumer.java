package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

public class TransactionConsumer {

    private static final String TOPIC = "input-topic";

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "transaction-consumer-group");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Start from earliest if no offset exists
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Auto commit (simple setup)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        ObjectMapper mapper = new ObjectMapper();

        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("Starting consumer...");

        try {
            while (true) {

                ConsumerRecords<String, String> records =
                        consumer.poll(java.time.Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {

                    try {
                        Transaction tx = mapper.readValue(record.value(), Transaction.class);

                        // 🔥 Simple transformation
                        String riskLevel = classifyRisk(tx.amount);
                        String formattedTime = Instant.ofEpochMilli(tx.timestamp).toString();

                        String output = String.format(
                                "User=%s | Tx=%s | Amount=%.2f %s | Risk=%s | Time=%s | Partition=%d Offset=%d",
                                tx.userId,
                                tx.transactionId,
                                tx.amount,
                                tx.currency,
                                riskLevel,
                                formattedTime,
                                record.partition(),
                                record.offset()
                        );

                        System.out.println(output);

                    } catch (Exception e) {
                        System.err.println("Failed to process record: " + record.value());
                        e.printStackTrace();
                    }
                }
            }

        } finally {
            consumer.close();
        }
    }

    private static String classifyRisk(double amount) {
        if (amount > 800) return "HIGH";
        if (amount > 300) return "MEDIUM";
        return "LOW";
    }
}