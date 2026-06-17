FROM eclipse-temurin:21-jre

WORKDIR /app

COPY target/reprocess-producer-2.0.0.jar app.jar

ENTRYPOINT ["java","-jar","app.jar"]