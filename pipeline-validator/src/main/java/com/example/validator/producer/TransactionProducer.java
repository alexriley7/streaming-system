package com.example.validator.producer;

import com.example.avro.TransactionAvro;
import com.example.validator.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TransactionProducer {

    private final KafkaProducer<String, TransactionAvro> producer;

    private final String topic;

    public TransactionProducer(KafkaConfig config) {

        this.producer = config.transactionProducer();

        this.topic = config.getTransactionTopic();

    }

    public void sendTransaction(
            String userId,
            String transactionId,
            double amount,
            String currency
    ) throws Exception {

        TransactionAvro event = TransactionAvro.newBuilder()

                .setEventId("validator-event")

                .setUserId(userId)

                .setTransactionId(transactionId)

                .setAmount(amount)

                .setCurrency(currency)

                .setTimestamp(System.currentTimeMillis())

                .setTimestampB(0L)

                .build();

        ProducerRecord<String, TransactionAvro> record =
                new ProducerRecord<>(
                        topic,
                        userId,
                        event
                );

        producer.send(record).get();

        System.out.println("Transaction sent.");

    }

}