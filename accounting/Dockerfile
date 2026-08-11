# ==========================================
# Stage 1: Build Stage
# ==========================================

FROM maven:3.9.6-eclipse-temurin-21-alpine AS builder

WORKDIR /app

# Copy pom.xml first to leverage Docker layer caching for dependencies
COPY pom.xml .

# Download dependencies offline (cached unless pom.xml changes)
RUN mvn dependency:go-offline -B

# Copy source code and build the application executable JAR
COPY src ./src
RUN mvn clean package -DskipTests

# ==========================================
# Stage 2: Runtime Stage
# ==========================================
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

# Create a non-root system user for improved container security
RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring

# Copy the built JAR file from the builder stage
COPY --from=builder /app/target/*.jar app.jar

# Expose default Spring Boot application port
EXPOSE 8080

# Configure JVM flags for container environments and run the app
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]