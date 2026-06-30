package com.example.validator.producer;

import com.example.validator.config.KafkaConfig;
import com.example.validator.model.Profile;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.UUID;

public class ProfileProducer {

    private final KafkaProducer<String, String> producer;

    private final ObjectMapper mapper;

    private final String topic;

    public ProfileProducer(KafkaConfig config) {

        this.producer = config.profileProducer();

        this.mapper = config.getObjectMapper();

        this.topic = config.getProfileTopic();

    }

    public void sendProfile(

            String userId,

            String name,

            String country

    ) throws Exception {

        Profile profile = new Profile(

                UUID.randomUUID().toString(),

                userId,

                name,

                country

        );

        String json =
                mapper.writeValueAsString(profile);

        ProducerRecord<String, String> record =

                new ProducerRecord<>(

                        topic,

                        userId,

                        json

                );

        producer.send(record).get();

        System.out.println(json);

    }

}