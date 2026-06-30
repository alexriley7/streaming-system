package com.example.validator.validation;

public class ValidationResult {

    private final boolean success;

    private final String message;

    public ValidationResult(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }

    public static ValidationResult success() {
        return new ValidationResult(true, "Validation succeeded.");
    }

    public static ValidationResult failure(String message) {
        return new ValidationResult(false, message);
    }

}