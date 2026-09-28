FROM eclipse-temurin:17-jre

WORKDIR /app
COPY target/scala-2.13/pekko-connectors-iceberg.jar app.jar

ENV KAFKA_SECURITY_PROTOCOL=PLAINTEXT
EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
