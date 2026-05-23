package com.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionProducer {

    private static final String TOPIC = "input-topic";

    // 1 event every 60 seconds
    private static final long PRODUCE_INTERVAL_MS = 60_000;

    private static final Random random = new Random();

    private static final ObjectMapper mapper =
            new ObjectMapper();

    // ========================================================
    // EVENT ID COUNTER
    // ========================================================

    // Generates:
    // 00000001
    // 00000002
    // ...
    // 99999999

    private static final AtomicInteger EVENT_COUNTER =
            new AtomicInteger(1);

    private static final int MAX_EVENT_ID =
            99_999_999;

    public static void main(String[] args) {

        Properties props = new Properties();

        String broker = System.getenv()
                .getOrDefault(
                        "KAFKA_BOOTSTRAP_SERVERS",
                        "localhost:9092"
                );

        // ====================================================
        // KAFKA CONFIG
        // ====================================================

        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                broker
        );

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

        // ====================================================
        // FEATURE FLAG
        // ====================================================

        boolean enabled = Boolean.parseBoolean(
                System.getenv()
                        .getOrDefault(
                                "ENABLE_PRODUCER",
                                "false"
                        )
        );

        System.out.println("==================================");
        System.out.println("Starting transaction producer...");
        System.out.println("ENABLE_PRODUCER = " + enabled);
        System.out.println("Rate = 1 transaction per minute");
        System.out.println("EventID range: 00000001 -> 99999999");
        System.out.println("==================================");

        try {

            // -------------------------------------------------
            // DISABLED MODE
            // -------------------------------------------------

            if (!enabled) {

                System.out.println(
                        "Producer DISABLED. Idling..."
                );

                while (true) {
                    Thread.sleep(60_000);
                }
            }

            // -------------------------------------------------
            // PRODUCER LOOP
            // -------------------------------------------------

            while (true) {

                Transaction tx =
                        generateTransaction();

                String key = tx.userId;

                String value =
                        mapper.writeValueAsString(tx);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(
                                TOPIC,
                                key,
                                value
                        );

                producer.send(
                        record,
                        (metadata, exception) -> {

                            if (exception != null) {

                                System.err.println(
                                        "Failed to send message"
                                );

                                exception.printStackTrace();

                            } else {

                                System.out.println(
                                        "Produced eventId=" +
                                        tx.eventId +
                                        " | transactionId=" +
                                        tx.transactionId +
                                        " | partition=" +
                                        metadata.partition() +
                                        " | offset=" +
                                        metadata.offset()
                                );
                            }
                        }
                );

                Thread.sleep(PRODUCE_INTERVAL_MS);
            }

        } catch (Exception e) {

            e.printStackTrace();

        } finally {

            producer.close();
        }
    }

    private static Transaction generateTransaction() {

        // ====================================================
        // EVENT ID GENERATION
        // ====================================================

        int currentId =
                EVENT_COUNTER.getAndIncrement();

        // STOP after 99999999

        if (currentId > MAX_EVENT_ID) {

            throw new RuntimeException(
                    "Event ID range exhausted. " +
                    "Maximum EventID 99999999 reached."
            );
        }

        // Format:
        // 00000001
        // 00000002
        // ...

        String eventId =
                String.format("%08d", currentId);

        // ====================================================
        // RANDOM TRANSACTION DATA
        // ====================================================

        String userId =
                "user-" + random.nextInt(100);

        String txId =
                UUID.randomUUID().toString();

        double amount =
                Math.round(
                        random.nextDouble() * 1000 * 100.0
                ) / 100.0;

        String currency = "USDT";

        long timestamp =
                System.currentTimeMillis();

        // ====================================================
        // CREATE TRANSACTION
        // ====================================================

        Transaction tx = new Transaction(
                userId,
                txId,
                amount,
                currency,
                timestamp
        );

        // ADD EVENT ID

        tx.eventId = eventId;

        return tx;
    }
}