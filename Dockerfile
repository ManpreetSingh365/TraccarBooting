# Use official OpenJDK image
FROM eclipse-temurin:21-jdk-alpine

# Set working directory
WORKDIR /app

# Copy built JAR from target folder
COPY target/device-gateway-service-1.0.0-SNAPSHOT.jar app.jar

# Expose app port
EXPOSE 8080

# Run the application
ENTRYPOINT ["java","-jar","app.jar"]
