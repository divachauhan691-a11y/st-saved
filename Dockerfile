FROM eclipse-temurin:17-jdk-alpine AS build
WORKDIR /app
COPY gradlew .
COPY gradle gradle
COPY build.gradle.kts .
COPY settings.gradle.kts .
COPY src src
RUN chmod +x gradlew && ./gradlew shadowJar

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
RUN apk add --no-cache ffmpeg
COPY --from=build /app/build/libs/XhRec-all.jar .
RUN adduser -D xhrec
USER xhrec
EXPOSE 8090
ENV TELEGRAM_BOT_TOKEN=""
ENV TELEGRAM_CHANNEL_ID=""
ENV TELEGRAM_ALLOWED_USERS=""
CMD ["java", "-jar", "XhRec-all.jar", "-p", "8090", "-o", "out", "-t", "tmp"]
