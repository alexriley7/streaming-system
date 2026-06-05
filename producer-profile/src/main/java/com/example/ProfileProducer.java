package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class ProfileProducer {

    private static final String TOPIC = "profiles-input-topic";
    private static final String BUCKET = "events";

    private static final String[] NAMES = {
            "Alice","Bob","Carlos","David","Emma",
            "Fernando","Grace","Henry","Isabella","Jack"
    };

    private static final String[] COUNTRIES = {
            "Argentina","Brazil","USA","Canada","Germany",
            "Spain","France","Mexico","Chile","Uruguay"
    };

    public static void main(String[] args) throws Exception {

        String broker = System.getenv()
                .getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        // ---------------- Kafka config ----------------
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // ---------------- JSON ----------------
        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random();

        // ---------------- Moto S3 client ----------------
        S3Client s3Client = S3Client.builder()
                .endpointOverride(URI.create("http://moto-s3.default.svc.cluster.local:5000"))
                .region(Region.US_EAST_1)
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create("test", "test")
                        )
                )
                .serviceConfiguration(
                        S3Configuration.builder()
                                .pathStyleAccessEnabled(true)
                                .build()
                )
                .build();

        while (true) {

            boolean enabled = Boolean.parseBoolean(
                    System.getenv().getOrDefault("ENABLE_PRODUCER", "true")
            );

            if (!enabled) {
                System.out.println("ENABLE_PRODUCER=false, producer paused");
                Thread.sleep(30000);
                continue;
            }

            String userId = "user-" + (random.nextInt(100) + 1);

            Profile profile = new Profile(
                    UUID.randomUUID().toString(),
                    userId,
                    NAMES[random.nextInt(NAMES.length)],
                    COUNTRIES[random.nextInt(COUNTRIES.length)]
            );

            String json = mapper.writeValueAsString(profile);

            // ---------------- Kafka write ----------------
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC, userId, json);

            producer.send(record);

            // ---------------- S3 (Moto) write ----------------
            String key = "profiles/" + userId + "/" + profile.getEventId() + ".json";

            s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(BUCKET)
                            .key(key)
                            .contentType("application/json")
                            .build(),
                    RequestBody.fromString(json)
            );

            System.out.println("PROFILE SENT -> " + json);
            System.out.println("S3 STORED -> s3://" + BUCKET + "/" + key);

            Thread.sleep(10000);
        }
    }
}