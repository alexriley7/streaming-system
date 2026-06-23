package com.example;

//import com.example.TransactionAvro;
//import src.main.avro.TransactionAvro;

import com.example.avro.TransactionAvro;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;


import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.time.LocalDate;

public class TransactionProducer {

    private static final String TOPIC = "input-topic-debug-a4";

    // 1 event every 60 seconds
    private static final long PRODUCE_INTERVAL_MS = 1_000;

    private static final Random random = new Random();

    private static final ObjectMapper mapper =
            new ObjectMapper();

    // ========================================================
    // EVENT ID COUNTER #
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

    // ==========================================
    // S3 CLIENT
    // ==========================================

        S3Client s3Client =
                S3Client.builder()
                        .endpointOverride(
                                URI.create(
                                        "http://moto-s3.default.svc.cluster.local:5000"
                                )
                        )
                        .region(Region.US_EAST_1)
                        .forcePathStyle(true)
                        .credentialsProvider(
                                StaticCredentialsProvider.create(
                                        AwsBasicCredentials.create(
                                                "test",
                                                "test"
                                        )
                                )
                        )
                        .build();   

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

//changed to Schema Registry:

        //props.put(
        //        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        //        StringSerializer.class.getName()
        //);




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


        props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "io.confluent.kafka.serializers.KafkaAvroSerializer"
        );

        props.put(
        "schema.registry.url",
        "http://schema-registry.kafka:8089"
        );

        System.out.println(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        System.out.println(props.get("schema.registry.url"));

        KafkaProducer<String, TransactionAvro> producer =
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

            /*while (true) {

                Transaction tx =
                        generateTransaction();

                String key = tx.userId;

                String value =
                        mapper.writeValueAsString(tx);

                // ==========================================
                // WRITE TO S3 ##
                // ==========================================

                try {

                    LocalDate today = LocalDate.now();

                    String objectKey =
                            String.format(
                                    "transactions/debug/a4/%04d/%02d/%02d/%s.json",
                                    today.getYear(),
                                    today.getMonthValue(),
                                    today.getDayOfMonth(),
                                    tx.eventId
                            );

                    PutObjectRequest putRequest =
                            PutObjectRequest.builder()
                                    .bucket("events")
                                    .key(objectKey)
                                    .contentType("application/json")
                                    .build();

                    s3Client.putObject(
                            putRequest,
                            RequestBody.fromString(value)
                    );

                    System.out.println(
                            "Saved to S3: s3://events/" +
                            objectKey
                    );

                } catch (Exception e) {

                    System.err.println(
                            "Failed to persist event to S3"
                    );

                    e.printStackTrace();
                }

                // ==========================================
                // WRITE TO KAFKA
                // ==========================================

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
            */

           // -------------------------------------------------
                // PRODUCER LOOP#
                // -------------------------------------------------

                while (true) {

                // Existing POJO
                Transaction tx = generateTransaction();

                // ==========================================
                // CREATE AVRO OBJECT
                // ==========================================

                TransactionAvro avroTx =
                        TransactionAvro.newBuilder()
                                .setEventId(tx.eventId)
                                .setUserId(tx.userId)
                                .setTransactionId(tx.transactionId)
                                .setAmount(tx.amount)
                                .setCurrency(tx.currency)
                                .setTimestamp(tx.timestamp)
                                .setTimestamp_b(tx.timestamp_b)
                                .build();

                //String key = avroTx.getUserId();
                String key = avroTx.getUserId().toString();

                // ==========================================
                // JSON ONLY FOR S3
                // ==========================================

                String json =
                        mapper.writeValueAsString(tx);

                // ==========================================
                // WRITE TO S3
                // ==========================================

                try {

                        LocalDate today = LocalDate.now();

                        String objectKey =
                                String.format(
                                        "transactions/debug/a4/%04d/%02d/%02d/%s.json",
                                        today.getYear(),
                                        today.getMonthValue(),
                                        today.getDayOfMonth(),
                                        tx.eventId
                                );

                        PutObjectRequest putRequest =
                                PutObjectRequest.builder()
                                        .bucket("events")
                                        .key(objectKey)
                                        .contentType("application/json")
                                        .build();

                        s3Client.putObject(
                                putRequest,
                                RequestBody.fromString(json)
                        );

                        System.out.println(
                                "Saved to S3: s3://events/" +
                                objectKey
                        );

                } catch (Exception e) {

                        System.err.println(
                                "Failed to persist event to S3"
                        );

                        e.printStackTrace();
                }

                // ==========================================
                // WRITE AVRO TO KAFKA
                // ==========================================

                System.out.println(avroTx.getSchema().toString(true));

                ProducerRecord<String, TransactionAvro> record =
                        new ProducerRecord<>(
                                TOPIC,
                                key,
                                avroTx
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
                                        avroTx.getEventId() +
                                        " | transactionId=" +
                                        avroTx.getTransactionId() +
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
                    s3Client.close();
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

        long timestamp_b =
                System.currentTimeMillis();

        // ====================================================
        // CREATE TRANSACTION
        // ====================================================

        Transaction tx = new Transaction(
                userId,
                txId,
                amount,
                currency,
                timestamp,
                timestamp_b
        );

        // ADD EVENT ID

        tx.eventId = eventId;

        return tx;
    }
}