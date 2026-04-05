FROM bellsoft/liberica-openjdk-alpine:21

WORKDIR /app

COPY target/grpc-tarantool-kv-1.0-SNAPSHOT.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]