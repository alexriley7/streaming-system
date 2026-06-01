package com.example;

class Profile {

    public String profileId;
    public String userId;
    public String name;
    public String country;

    public Profile(
            String profileId,
            String userId,
            String name,
            String country
    ) {
        this.profileId = profileId;
        this.userId = userId;
        this.name = name;
        this.country = country;
    }
}