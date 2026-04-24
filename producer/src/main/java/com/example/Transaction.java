package com.example;

class Transaction {
    public String userId;
    public String transactionId;
    public double amount;
    public String currency;
    public long timestamp;

    public Transaction(String userId, String transactionId, double amount, String currency, long timestamp) {
        this.userId = userId;
        this.transactionId = transactionId;
        this.amount = amount;
        this.currency = currency;
        this.timestamp = timestamp;
    }
}