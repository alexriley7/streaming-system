package com.example;

public class Profile {

    private String eventId;
    private String userId;
    private String name;
    private String country;

    public Profile() {}

    public Profile(String eventId, String userId, String name, String country) {
        this.eventId = eventId;
        this.userId = userId;
        this.name = name;
        this.country = country;
    }

    public String getEventId() {
        return eventId;
    }

    public String getUserId() {
        return userId;
    }

    public String getName() {
        return name;
    }

    public String getCountry() {
        return country;
    }
}