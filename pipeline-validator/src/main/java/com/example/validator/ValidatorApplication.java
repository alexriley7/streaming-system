package com.example.validator;

import com.example.validator.config.KafkaConfig;
import com.example.validator.consumer.ShadowOutputConsumer;
import com.example.validator.model.OutputEvent;
import com.example.validator.producer.ProfileProducer;
import com.example.validator.producer.TransactionProducer;
import com.example.validator.validation.EventValidator;
import com.example.validator.validation.ValidationResult;

import java.util.UUID;

public class ValidatorApplication {

    public static void main(String[] args) {

        try {

            KafkaConfig kafkaConfig = new KafkaConfig();

            TransactionProducer transactionProducer =
                    new TransactionProducer(kafkaConfig);

            ProfileProducer profileProducer =
                    new ProfileProducer(kafkaConfig);

            ShadowOutputConsumer consumer =
                    new ShadowOutputConsumer(kafkaConfig);

            EventValidator validator =
                    new EventValidator();

            // ---------------------------------------
            // Generate unique identifiers
            // ---------------------------------------

            String userId = "validator-" + UUID.randomUUID();

            String transactionId = UUID.randomUUID().toString();

            // ---------------------------------------
            // Send profile ##
            // ---------------------------------------

            profileProducer.sendProfile(
                    userId,
                    "Pipeline Validator",
                    "Argentina"
            );

            System.out.println("Profile sent.");

            // Small delay so Flink can populate its state.
            // Later we can replace this with smarter polling.

            Thread.sleep(3000);

            // ---------------------------------------
            // Send transaction
            // ---------------------------------------

            transactionProducer.sendTransaction(
                    userId,
                    transactionId,
                    100.0,
                    "USD"
            );

            System.out.println("Transaction sent.");

            // ---------------------------------------
            // Wait for enriched event
            // ---------------------------------------

            OutputEvent output =
                    consumer.waitForTransaction(transactionId, 30);

            // ---------------------------------------
            // Validate
            // ---------------------------------------

            ValidationResult result =
                    validator.validate(
                            output,
                            userId,
                            "Pipeline Validator",
                            "Argentina",
                            100.0,
                            "USD"
                    );

            if (!result.isSuccess()) {

                System.err.println(result.getMessage());

                System.exit(1);

            }

            System.out.println("Pipeline validation PASSED.");

            System.exit(0);

        } catch (Exception ex) {

            ex.printStackTrace();

            System.exit(1);

        }

    }

}