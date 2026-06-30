package com.example.validator.consumer;

import com.example.validator.config.KafkaConfig;
import com.example.validator.model.OutputEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;

public class ShadowOutputConsumer {

    private final KafkaConsumer<String, String> consumer;

    private final ObjectMapper mapper;

    private final String topic;

    public ShadowOutputConsumer(KafkaConfig config) {

        this.consumer = config.shadowConsumer();

        this.mapper = config.getObjectMapper();

        this.topic = config.getShadowOutputTopic();

        consumer.subscribe(
                Collections.singletonList(topic)
        );

    }

    public OutputEvent waitForTransaction(

            String transactionId,

            int timeoutSeconds

    ) throws Exception {

        long deadline =
                System.currentTimeMillis()
                        + timeoutSeconds * 1000L;

        while (System.currentTimeMillis() < deadline) {

            ConsumerRecords<String, String> records =

                    consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records) {

                OutputEvent event =
                        mapper.readValue(
                                record.value(),
                                OutputEvent.class
                        );

                if (transactionId.equals(event.getTransactionId())) {

                    return event;

                }

            }

        }

        throw new RuntimeException(

                "Timeout waiting for transaction "

                        + transactionId

        );

    }

}