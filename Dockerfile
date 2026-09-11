# Stage 1: Build shaded JAR
FROM maven:3.9.6-eclipse-temurin-21 AS builder

WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src ./src
RUN mvn clean package -DskipTests

# Stage 2: Runtime image
FROM eclipse-temurin:21-jre-jammy

LABEL maintainer="Kanak Prabhakar <140477053+Kanak234@users.noreply.github.com>"
LABEL org.opencontainers.image.source="https://github.com/Kanak234/raft-consensus-protocol-engine"
LABEL org.opencontainers.image.description="Raft Consensus Protocol Engine in Java 21 LTS"

WORKDIR /app
COPY --from=builder /build/target/raft-consensus-engine.jar /app/raft-consensus-engine.jar

ENTRYPOINT ["java", "-jar", "/app/raft-consensus-engine.jar"]
CMD ["--bench", "1000"]
