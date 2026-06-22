package com.example;

class Transaction {

    public String eventId;

    public String userId;
    public String transactionId;
    public double amount;
    public String currency_type;
    public long timestamp;

    public Transaction(
            String userId,
            String transactionId,
            double amount,
            String currency_type,
            long timestamp
    ) {
        this.userId = userId;
        this.transactionId = transactionId;
        this.amount = amount;
        this.currency_type = currency_type;
        this.timestamp = timestamp;
    }
}