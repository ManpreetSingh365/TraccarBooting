# Use official Eclipse Temurin OpenJDK 21 Alpine image
FROM eclipse-temurin:21-jdk-alpine

# Set working directory inside container
WORKDIR /app

# Copy the built jar from the host's target directory into the container
COPY target/device-gateway-service-1.0.0-SNAPSHOT.jar app.jar

# Expose port 8080 for Spring Boot application
EXPOSE 8080

# Run the Spring Boot application using java -jar
ENTRYPOINT ["java", "-jar", "app.jar"]