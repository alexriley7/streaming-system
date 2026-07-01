package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import software.amazon.awssdk.core.sync.RequestBody;

import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.s3.S3Client;

import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;

import java.time.Instant;

import java.util.Properties;
import java.util.UUID;

// add profile state management

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

// expose flink metric to prometheus #


import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;






// ============================================================
// TRANSACTION POJO
// ============================================================

class Transaction {

    public String eventId;

    public String userId;
    public String transactionId;

    public double amount;

    public String currency;

    public long timestamp;
}


// ============================================================
// ENRICHED TRANSACTION
// ============================================================

/*class EnrichedTransaction extends Transaction {

    public boolean highValue;

    public String normalizedCurrency;
}
*/

class EnrichedTransaction extends Transaction {

    public String profileId;

    public String name;

    public String country;
}

// add enrichment functions

class Profile {

    public String profileId;

    public String userId;

    public String name;

    public String country;

    public String eventId;
}

class ProfileEnrichmentFunction
        extends KeyedCoProcessFunction<
                String,
                Transaction,
                Profile,
                EnrichedTransaction> {

    private transient ValueState<Profile> profileState;

    
    private transient Counter nullProfileCounter;
    private transient Counter enrichedCounter;
    private transient Counter profileUpdateCounter;



    @Override
    public void open(Configuration parameters) {

        ValueStateDescriptor<Profile> descriptor =
                new ValueStateDescriptor<>(
                        "profile-state",
                        Profile.class
                );

        profileState =
                getRuntimeContext().getState(descriptor);


        MetricGroup metrics =
                getRuntimeContext().getMetricGroup();

        nullProfileCounter =
                metrics.counter("null_profile_count");

        enrichedCounter =
                metrics.counter("successful_enrichment_count");

        profileUpdateCounter =
                metrics.counter("profile_update_count");




    }

    @Override
    public void processElement1(
            Transaction tx,
            Context ctx,
            Collector<EnrichedTransaction> out)
            throws Exception {

        Profile profile = profileState.value();

        EnrichedTransaction enriched =
                new EnrichedTransaction();

        enriched.eventId = tx.eventId;
        enriched.userId = tx.userId;
        enriched.transactionId = tx.transactionId;
        enriched.amount = tx.amount;
        enriched.currency = tx.currency;
        enriched.timestamp = tx.timestamp;

        if (profile != null) {

            enriched.profileId =
                    profile.profileId;

            enriched.name =
                    profile.name;

            enriched.country =
                    profile.country;

            enrichedCounter.inc();

        } else {

                nullProfileCounter.inc();

                System.out.println(
                        "No profile found for user "
                        + tx.userId
                );
        }

        out.collect(enriched);
    }

    @Override
    public void processElement2(
            Profile profile,
            Context ctx,
            Collector<EnrichedTransaction> out)
            throws Exception {

        profileState.update(profile);

        profileUpdateCounter.inc();

        System.out.println(
                "Updated profile for "
                        + profile.userId
        );
    }
}


// ============================================================
// MAIN JOB #LastUpdated
// ============================================================

public class ShadowFlinkTransformationJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

        env.enableCheckpointing(30000); // added

        ObjectMapper mapper = new ObjectMapper();

        // ====================================================
        // KAFKA CONFIG
        // ====================================================

        String brokers = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS",
                "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
        );

        String groupId = System.getenv().getOrDefault(
                "GROUP_ID",
                "shadow-flink-consumer-v5"
        );

        // ====================================================
        // OUTPUT TOPIC
        // ====================================================

        String outputTopic = System.getenv().getOrDefault(
                "OUTPUT_TOPIC",
                "shadow-output-topic-debug-b1"
        );

        // ====================================================
        // FEATURE FLAG //
        // ====================================================

        boolean enabled = Boolean.parseBoolean(
                System.getenv()
                        .getOrDefault(
                                "ENABLE_JOB",
                                "true"
                        )
        );

        System.out.println("=================================");
        System.out.println("ENABLE_JOB = " + enabled);
        System.out.println("OUTPUT_TOPIC = " + outputTopic);
        System.out.println("GROUP_ID = " + groupId);
        System.out.println("BOOTSTRAP_SERVERS = " + brokers);
        System.out.println("=================================");

        // ====================================================
        // CONSUMER PROPERTIES
        // ====================================================

        Properties consumerProps = new Properties();

        consumerProps.setProperty(
                "bootstrap.servers",
                brokers
        );

        consumerProps.setProperty(
                "group.id",
                groupId
        );

        consumerProps.setProperty(
                "auto.offset.reset",
                "latest"
        );

        consumerProps.setProperty(
                "enable.auto.commit",
                "true"
        );

        // ====================================================
        // PRODUCER PROPERTIES
        // ====================================================

        Properties producerProps = new Properties();

        producerProps.setProperty(
                "bootstrap.servers",
                brokers
        );

        // ====================================================
        // SOURCE
        // ====================================================
        /*

        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>(
                        "input-topic",
                        new SimpleStringSchema(),
                        consumerProps
                );

        */


        FlinkKafkaConsumer<String> transactionConsumer =
        new FlinkKafkaConsumer<>(
                "input-topic-debug-b1",
                new SimpleStringSchema(),
                consumerProps
        );

        transactionConsumer.setStartFromLatest(); //added

        FlinkKafkaConsumer<String> profileConsumer =
                new FlinkKafkaConsumer<>(
                        "profiles-input-topic-debug-b1",
                        new SimpleStringSchema(),
                        consumerProps
                );

        profileConsumer.setStartFromEarliest(); //added



        // IMPORTANT:
        // IGNORE OLD HISTORICAL MESSAGES

        //consumer.setStartFromLatest();

        // ====================================================
        // TRANSFORMATION
        // ====================================================

/*
        var stream = env
                .addSource(consumer)
                .map(value -> {

                    try {

                        Transaction tx =
                                mapper.readValue(
                                        value,
                                        Transaction.class
                                );

                        EnrichedTransaction enriched =
                                new EnrichedTransaction();

                        // ------------------------------------
                        // COPY FIELDS
                        // ------------------------------------

                        enriched.eventId = tx.eventId;

                        enriched.userId = tx.userId;

                        enriched.transactionId =
                                tx.transactionId;

                        enriched.amount = tx.amount;

                        enriched.currency = tx.currency;

                        enriched.timestamp = tx.timestamp;

                        // ------------------------------------
                        // ENRICHMENT LOGIC
                        // ------------------------------------

                        enriched.highValue =
                                tx.amount > 500;

                        enriched.normalizedCurrency =
                                tx.currency.replace(
                                        "Hello ",
                                        "ShadowTestFinal"
                                );

                        // RETURN JSON STRING

                        return mapper.writeValueAsString(
                                enriched
                        );

                    } catch (Exception e) {

                        e.printStackTrace();

                        return null;
                    }
                })
                .filter(value -> value != null);

        */


       var transactions =
        env.addSource(transactionConsumer)
                .map(value -> {

                    try {

                        return mapper.readValue(
                                value,
                                Transaction.class
                        );

                    } catch (Exception e) {

                        e.printStackTrace();

                        return null;
                    }
                })
                .filter(v -> v != null);

        var profiles =
                env.addSource(profileConsumer)
                        .map(value -> {

                        try {

                                return mapper.readValue(
                                        value,
                                        Profile.class
                                );

                        } catch (Exception e) {

                                e.printStackTrace();

                                return null;
                        }
                        })
                        .filter(v -> v != null);

        var stream =
                transactions
                        .keyBy(tx -> tx.userId)
                        .connect(
                                profiles.keyBy(
                                        profile -> profile.userId
                                )
                        )
                        .process(
                                new ProfileEnrichmentFunction()
                        )
                        .map(value -> {

                        try {

                                return mapper.writeValueAsString(
                                        value
                                );

                        } catch (Exception e) {

                                e.printStackTrace();

                                return null;
                        }
                        })
                        .filter(v -> v != null);

        // ====================================================
        // KAFKA SINK
        // ====================================================

        if (enabled) {

            System.out.println(
                    "Feature flag enabled. " +
                    "Sinking to Kafka topic: " +
                    outputTopic
            );

            FlinkKafkaProducer<String> producer =
                    new FlinkKafkaProducer<>(
                            outputTopic,
                            new SimpleStringSchema(),
                            producerProps
                    );

            stream.addSink(producer);

        } else {

            System.out.println(
                    "Feature flag disabled"
            );
        }

        // ====================================================
        // MOTO S3 DIRECT SINK #
        // ====================================================

        stream.addSink(

                new RichSinkFunction<String>() {

                    private transient S3Client s3;

                    @Override
                    public void open(
                            Configuration parameters
                    ) {

                        s3 =
                                S3Client.builder()
                                        .endpointOverride(
                                                URI.create(
                                                        "http://moto-s3.default.svc.cluster.local:5000"
                                                )
                                        )
                                        .region(
                                                Region.US_EAST_1
                                        )
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

                        System.out.println(
                                "Moto S3 client initialized"
                        );
                    }

                    @Override
                    public void invoke(
                            String value,
                            Context context
                    ) {

                        try {

                            String key =
                                    "events/debug/b1"
                                    + Instant.now().toString()
                                    + "-"
                                    + UUID.randomUUID()
                                    + ".json";

                            s3.putObject(
                                    PutObjectRequest.builder()
                                            .bucket(
                                                    "shadow-flink-job"
                                            )
                                            .key(key)
                                            .contentType(
                                                    "application/json"
                                            )
                                            .build(),
                                    RequestBody.fromString(
                                            value
                                    )
                            );

                            System.out.println(
                                    "Uploaded event to Moto S3 -> "
                                    + key
                            );

                        } catch (Exception e) {

                            System.out.println(
                                    "Failed to upload event to Moto S3"
                            );

                            e.printStackTrace();
                        }
                    }
                }
        );

        System.out.println(
                "Moto S3 direct sink enabled"
        );

        // ====================================================
        // EXECUTE JOB#####
        // ====================================================

        env.execute(
                "Shadow Flink Transaction Enrichment Job"
        );
    }
}