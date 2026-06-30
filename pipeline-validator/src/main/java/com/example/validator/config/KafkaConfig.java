package com.example.validator.config;

import com.example.avro.TransactionAvro;

import com.example.validator.model.OutputEvent;
import com.example.validator.model.Profile;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;
import java.util.UUID;

public class KafkaConfig {

    private static final String DEFAULT_BOOTSTRAP =
            "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092";

    private static final String DEFAULT_SCHEMA_REGISTRY =
            "http://schema-registry.kafka.svc.cluster.local:8081";

    private final String bootstrapServers;

    private final String schemaRegistryUrl;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final String transactionTopic =
        System.getenv().getOrDefault(
                "TRANSACTION_TOPIC",
                "input-topic-debug-b1");

    private final String profileTopic =
            System.getenv().getOrDefault(
                    "PROFILE_TOPIC",
                    "profiles-input-topic-debug-b1");

    private final String shadowOutputTopic =
            System.getenv().getOrDefault(
                    "SHADOW_OUTPUT_TOPIC",
                    "shadow-output-topic-debug-b1");

    public KafkaConfig() {

        bootstrapServers = System.getenv()
                .getOrDefault(
                        "KAFKA_BOOTSTRAP_SERVERS",
                        DEFAULT_BOOTSTRAP
                );

        schemaRegistryUrl = System.getenv()
                .getOrDefault(
                        "SCHEMA_REGISTRY_URL",
                        DEFAULT_SCHEMA_REGISTRY
                );

    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public String getTransactionTopic() {
    return transactionTopic;
    }

    public String getProfileTopic() {
        return profileTopic;
    }

    public String getShadowOutputTopic() {
        return shadowOutputTopic;
    }

    /**
     * Transaction producer (Avro)
     */
    public KafkaProducer<String, TransactionAvro> transactionProducer() {

        Properties props = new Properties();

        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName()
        );

        props.put(
                "schema.registry.url",
                schemaRegistryUrl
        );

        props.put(
                "auto.register.schemas",
                true
        );

        return new KafkaProducer<>(props);

    }

    /**
     * Profile producer (JSON)
     */
    public KafkaProducer<String, String> profileProducer() {

        Properties props = new Properties();

        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        return new KafkaProducer<>(props);

    }

    /**
     * Output consumer (JSON)
     */
    public KafkaConsumer<String, String> shadowConsumer() {

        Properties props = new Properties();

        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        props.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false
        );

        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "pipeline-validator-" + UUID.randomUUID()
        );

        return new KafkaConsumer<>(props);

    }

}