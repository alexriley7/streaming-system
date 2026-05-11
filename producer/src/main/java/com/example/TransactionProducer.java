package com.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class TransactionProducer {

    private static final String TOPIC = "input-topic";
    private static final Random random = new Random();
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {

        Properties props = new Properties();

        String broker = System.getenv()
                .getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        // ✅ correct bootstrap usage ###
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // reliability
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // ✅ FEATURE FLAG
        boolean enabled = Boolean.parseBoolean(
                System.getenv().getOrDefault("ENABLE_PRODUCER", "true") // check if is working
        );

        System.out.println("Starting transaction producer...");
        System.out.println("ENABLE_PRODUCER = " + enabled);

        try {
            if (!enabled) {
                // 🧠 Keep pod alive but do nothing
                System.out.println("Producer DISABLED. Idling...");
                while (true) {
                    Thread.sleep(60000);
                }
            }

            // 🚀 Normal production loop
            while (true) {

                Transaction tx = generateTransaction();

                String key = tx.userId;
                String value = mapper.writeValueAsString(tx);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, key, value);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.println("Sent to partition " + metadata.partition() +
                                " offset " + metadata.offset());
                    }
                });

                // control throughput#
                Thread.sleep(200);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static Transaction generateTransaction() {
        String userId = "user-" + random.nextInt(100);
        String txId = UUID.randomUUID().toString();
        double amount = Math.round(random.nextDouble() * 1000 * 100.0) / 100.0;
        String currency = "Hello USDT";
        long timestamp = System.currentTimeMillis();

        return new Transaction(userId, txId, amount, currency, timestamp);
    }
}