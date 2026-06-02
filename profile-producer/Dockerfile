FROM eclipse-temurin:17-jdk-jammy

WORKDIR /app

COPY target/producer-profile-1.0-SNAPSHOT.jar producer-profile-1.0-SNAPSHOT.jar

ENTRYPOINT ["java","-jar","app.jar"]