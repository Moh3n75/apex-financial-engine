FROM eclipse-temurin:21-jre-alpine

RUN addgroup -S apex && adduser -S apex -G apex

WORKDIR /app

COPY target/reporting-cell.jar app.jar

RUN chown apex:apex /app/app.jar

USER apex

EXPOSE 8082

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
