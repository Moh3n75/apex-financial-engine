# Cell-owned image definition only. Build the Spring Boot JAR first:
#   mvn -DskipTests package
# Then:
#   docker build -t apex-cell-template:local .
#
# Do not bake secrets or environment-specific hosts into this image.

FROM eclipse-temurin:21-jre-alpine

RUN addgroup -S apex && adduser -S apex -G apex

WORKDIR /app

COPY target/apex-cell-template.jar app.jar

RUN chown apex:apex /app/app.jar

USER apex

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
