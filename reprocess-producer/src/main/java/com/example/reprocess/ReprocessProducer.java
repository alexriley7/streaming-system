package com.example.reprocess;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.util.Properties;

public class ReprocessProducer {

    private static final String BUCKET = "events";

    private static final String PROFILE_PREFIX = "profiles/debug/a4";
    private static final String TX_PREFIX = "transactions/debug/a4";

    private static final String PROFILE_TOPIC = "repro-profiles-topic-a4";
    private static final String TX_TOPIC = "repro-transactions-topic-a4";

    public static void main(String[] args) {

        S3Client s3 = S3Client.builder()
                .endpointOverride(
                        URI.create("http://moto-s3.default.svc.cluster.local:5000"))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(
                                        "test",
                                        "test")))
                .build();

        Properties props = new Properties();

        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092");

        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(props);

        replayPrefix(
                s3,
                producer,
                PROFILE_PREFIX,
                PROFILE_TOPIC);

        replayPrefix(
                s3,
                producer,
                TX_PREFIX,
                TX_TOPIC);

        producer.flush();
        producer.close();
        s3.close();

        System.out.println("Replay completed");
    }

    private static void replayPrefix(
            S3Client s3,
            KafkaProducer<String, String> producer,
            String prefix,
            String topic) {

        ListObjectsV2Request listRequest =
                ListObjectsV2Request.builder()
                        .bucket(BUCKET)
                        .prefix(prefix)
                        .build();

        ListObjectsV2Response response =
                s3.listObjectsV2(listRequest);

        for (S3Object object : response.contents()) {

            if (object.key().endsWith("/")) {
                continue;
            }

            ResponseBytes<GetObjectResponse> bytes =
                    s3.getObjectAsBytes(
                            GetObjectRequest.builder()
                                    .bucket(BUCKET)
                                    .key(object.key())
                                    .build());

            String payload = bytes.asUtf8String();

            producer.send(
                    new ProducerRecord<>(
                            topic,
                            payload));

            System.out.printf(
                    "Replayed %s -> %s%n",
                    object.key(),
                    topic);
        }
    }
}