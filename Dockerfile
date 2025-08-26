# Stage 1: build with Maven + JDK 21
FROM maven:3.9.4-eclipse-temurin-21 AS build
WORKDIR /app
COPY . /app
RUN mvn -DskipTests package

# Stage 2: runtime with Temurin 21
FROM eclipse-temurin:21-jdk-alpine
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
EXPOSE 8080
EXPOSE 5023
ENTRYPOINT ["java","-jar","app.jar"]