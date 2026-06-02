package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class ProfileProducer {

    private static final String TOPIC = "profiles-input-topic";

    private static final String[] NAMES = {
            "Alice",
            "Bob",
            "Carlos",
            "David",
            "Emma",
            "Fernando",
            "Grace",
            "Henry",
            "Isabella",
            "Jack"
    };

    private static final String[] COUNTRIES = {
            "Argentina",
            "Brazil",
            "USA",
            "Canada",
            "Germany",
            "Spain",
            "France",
            "Mexico",
            "Chile",
            "Uruguay"
    };

    public static void main(String[] args) throws Exception {

        String broker = System.getenv()
                .getOrDefault(
                        "KAFKA_BOOTSTRAP_SERVERS",
                        "localhost:9092"
                );

        Properties props = new Properties();

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

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(props);

        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random();

        // Generate profiles for user-1 through user-100
        while (true) {

            String userId =
                    "user-" + (random.nextInt(100) + 1);

            Profile profile = new Profile(
                    UUID.randomUUID().toString(),
                    userId,
                    NAMES[random.nextInt(NAMES.length)],
                    COUNTRIES[random.nextInt(COUNTRIES.length)]
            );

            String json =
                    mapper.writeValueAsString(profile);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(
                            TOPIC,
                            userId,   // Kafka key
                            json
                    );

            producer.send(record);

            System.out.println(
                    "PROFILE SENT -> " + json
            );

            Thread.sleep(10000); // every 10 seconds
        }
    }
}