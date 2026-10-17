# Build JAR first: mvn -DskipTests package
# Then: docker build -t apex/customer-cell:m21 .
#
# Runtime DB hostname is NOT localhost from inside the container.
# Example: CELL_DB_URL=jdbc:postgresql://host.docker.internal:5434/customer_db

FROM eclipse-temurin:21-jre-alpine

RUN addgroup -S apex && adduser -S apex -G apex

WORKDIR /app

COPY target/customer-cell.jar app.jar

RUN chown apex:apex /app/app.jar

USER apex

EXPOSE 8081

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
