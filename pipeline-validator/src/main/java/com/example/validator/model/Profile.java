package com.example.validator.model;

public class Profile {

    private String eventId;

    private String userId;

    private String name;

    private String country;

    public Profile() {
    }

    public Profile(
            String eventId,
            String userId,
            String name,
            String country
    ) {
        this.eventId = eventId;
        this.userId = userId;
        this.name = name;
        this.country = country;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

}