FROM eclipse-temurin:21-jre-jammy
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ARG JAR
COPY ${JAR} /app/app.jar
USER 10001:10001
ENTRYPOINT ["java","-XX:MaxRAMPercentage=65","-jar","/app/app.jar"]
