FROM eclipse-temurin:23-jdk-alpine AS build
WORKDIR /app

RUN apk add --no-cache maven

# Download dependencies first so this layer is cached until pom.xml changes
COPY pom.xml .
RUN mvn -B dependency:go-offline

COPY src src

# The tests run as part of the image build, so a broken change never produces an image.
# Use:  docker build --build-arg SKIP_TESTS=true .   to skip them.
ARG SKIP_TESTS=false
RUN mvn -B package -DskipTests=${SKIP_TESTS} -Ddir=/tmp/codecrafters-build-redis-java

FROM eclipse-temurin:23-jre-alpine
WORKDIR /app

RUN addgroup -S redis && adduser -S redis -G redis
COPY --from=build /tmp/codecrafters-build-redis-java/codecrafters-redis.jar .
USER redis

EXPOSE 6379
ENTRYPOINT ["java", "-jar", "codecrafters-redis.jar"]
