package com.example.validator.validation;

import com.example.validator.model.OutputEvent;

public class EventValidator {

    public ValidationResult validate(
            OutputEvent event,
            String expectedUserId,
            String expectedName,
            String expectedCountry,
            double expectedAmount,
            String expectedCurrency
    ) {

        if (event == null) {
            return ValidationResult.failure("Output event is null.");
        }

        if (!expectedUserId.equals(event.getUserId())) {
            return ValidationResult.failure(
                    "Unexpected userId. Expected="
                            + expectedUserId
                            + " Actual="
                            + event.getUserId()
            );
        }

        if (!expectedName.equals(event.getName())) {
            return ValidationResult.failure(
                    "Unexpected name. Expected="
                            + expectedName
                            + " Actual="
                            + event.getName()
            );
        }

        if (!expectedCountry.equals(event.getCountry())) {
            return ValidationResult.failure(
                    "Unexpected country. Expected="
                            + expectedCountry
                            + " Actual="
                            + event.getCountry()
            );
        }

        if (event.getAmount() != expectedAmount) {
            return ValidationResult.failure(
                    "Unexpected amount. Expected="
                            + expectedAmount
                            + " Actual="
                            + event.getAmount()
            );
        }

        if (!expectedCurrency.equals(event.getCurrency())) {
            return ValidationResult.failure(
                    "Unexpected currency. Expected="
                            + expectedCurrency
                            + " Actual="
                            + event.getCurrency()
            );
        }

        if (event.getTransactionId() == null ||
                event.getTransactionId().isBlank()) {

            return ValidationResult.failure(
                    "TransactionId is null."
            );
        }

        return ValidationResult.success();

    }

}