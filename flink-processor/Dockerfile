FROM openjdk:8u151-jdk-alpine3.7

# Install Bash
RUN apk add --no-cache bash libc6-compat

# Copy resources
WORKDIR /
COPY target/flink-kafka2postgres-1.0-SNAPSHOT-jar-with-dependencies.jar flink-processor.jar

# Wait for Zookeeper and Kafka to be available and run application
CMD  java -Xmx512m -jar flink-processor.jar