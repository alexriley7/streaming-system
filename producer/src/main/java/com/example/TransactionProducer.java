package com.example;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class TransactionProducer {

    private static final String TOPIC = "input-topic";
    private static final Random random = new Random();
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {

        Properties props = new Properties();

        String broker = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        //modified this line:
        // 
        // props.put("bootstrap.servers", broker);

        //added this line:

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);

        

        // this line may mess with kafka k8 cluster connection :
        // 
        // props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 🔥 Important for reliability # HEY HEY HEY #########
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        System.out.println("Starting transaction producer...");

        try {
            while (true) {

                Transaction tx = generateTransaction();

                String key = tx.userId;
                String value = mapper.writeValueAsString(tx);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, key, value);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.println("Sent to partition " + metadata.partition() +
                                " offset " + metadata.offset());
                    }
                });

                // control throughput
                Thread.sleep(200); // ~5 events/sec
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static Transaction generateTransaction() {
        String userId = "user-" + random.nextInt(100);
        String txId = UUID.randomUUID().toString();
        double amount = Math.round(random.nextDouble() * 1000 * 100.0) / 100.0;
        String currency = "Hello my dear";
        long timestamp = System.currentTimeMillis();

        return new Transaction(userId, txId, amount, currency, timestamp);
    }
}