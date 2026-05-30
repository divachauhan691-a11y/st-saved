FROM eclipse-temurin:17-jdk-alpine AS build
WORKDIR /app
COPY gradlew .
COPY gradle gradle
COPY build.gradle.kts .
COPY settings.gradle.kts .
COPY src src
RUN chmod +x gradlew && ./gradlew shadowJar

FROM eclipse-temurin:17-jre-alpine
WORKDIR /data
RUN apk add --no-cache ffmpeg
COPY --from=build /app/build/libs/XhRec-all.jar .
EXPOSE 8090
ENV TELEGRAM_BOT_TOKEN=""
ENV TELEGRAM_CHANNEL_ID=""
ENV TELEGRAM_ALLOWED_USERS=""
ENV MONGODB_URI=""
VOLUME /data/out /data/tmp /data/logs
CMD ["java", "-Xmx320m", "-Xms128m", "-jar", "XhRec-all.jar", "-p", "8090", "-o", "out", "-t", "tmp"]
