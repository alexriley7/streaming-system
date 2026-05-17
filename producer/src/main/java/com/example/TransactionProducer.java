package com.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class TransactionProducer {

    private static final String TOPIC = "input-topic";

    // 6 events per minute = 1 event every 10 seconds
    //private static final long PRODUCE_INTERVAL_MS = 10_000;

    private static final long PRODUCE_INTERVAL_MS = 10;

    private static final Random random = new Random();
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {

        Properties props = new Properties();

        String broker = System.getenv()
                .getOrDefault(
                        "KAFKA_BOOTSTRAP_SERVERS",
                        "localhost:9092"
                );

        // Kafka bootstrap server
        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                broker
        );

        // serializers
        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        // reliability
        props.put(
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                "true"
        );

        props.put(
                ProducerConfig.ACKS_CONFIG,
                "all"
        );

        props.put(
                ProducerConfig.RETRIES_CONFIG,
                Integer.MAX_VALUE
        );

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(props);

        // feature flag
        boolean enabled = Boolean.parseBoolean(
                System.getenv()
                        .getOrDefault(
                                "ENABLE_PRODUCER",
                                "true"
                        )
        );

        System.out.println("==================================");
        System.out.println("Starting transaction producer...");
        System.out.println("ENABLE_PRODUCER = " + enabled);
        System.out.println("Rate = 6 transactions per minute");
        System.out.println("==================================");

        try {

            if (!enabled) {

                System.out.println("Producer DISABLED. Idling...");

                while (true) {
                    Thread.sleep(60_000);
                }
            }

            // -------------------------------------------------#
            // PRODUCER LOOP
            // -------------------------------------------------

            while (true) {

                Transaction tx = generateTransaction();

                String key = tx.userId;

                String value =
                        mapper.writeValueAsString(tx);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(
                                TOPIC,
                                key,
                                value
                        );

                producer.send(record, (metadata, exception) -> {

                    if (exception != null) {

                        System.err.println(
                                "Failed to send message"
                        );

                        exception.printStackTrace();

                    } else {

                        System.out.println(
                                "Produced transaction: " +
                                tx.transactionId +
                                " | partition=" +
                                metadata.partition() +
                                " | offset=" +
                                metadata.offset()
                        );
                    }
                });

                // -------------------------------------------------
                // 6 EVENTS / MINUTE
                // -------------------------------------------------

                Thread.sleep(PRODUCE_INTERVAL_MS);
            }

        } catch (Exception e) {

            e.printStackTrace();

        } finally {

            producer.close();
        }
    }

    private static Transaction generateTransaction() {

        String userId =
                "user-" + random.nextInt(100);

        String txId =
                UUID.randomUUID().toString();

        double amount =
                Math.round(
                        random.nextDouble() * 1000 * 100.0
                ) / 100.0;

        String currency = "Hello USDT";

        long timestamp =
                System.currentTimeMillis();

        return new Transaction(
                userId,
                txId,
                amount,
                currency,
                timestamp
        );
    }
}