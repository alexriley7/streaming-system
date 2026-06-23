package com.example;

class Transaction {

    public String eventId;

    public String userId;
    public String transactionId;
    public double amount;
    public String currency_type;
    public long timestamp;
    public long timestamp_b;

    public Transaction(
            String userId,
            String transactionId,
            double amount,
            String currency_type,
            long timestamp,
            long timestamp_b
    ) {
        this.userId = userId;
        this.transactionId = transactionId;
        this.amount = amount;
        this.currency_type = currency_type;
        this.timestamp = timestamp;
        this.timestamp_b = timestamp_b;
    }
}