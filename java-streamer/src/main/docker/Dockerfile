FROM openjdk:17-alpine
VOLUME /tmp
ADD java-streamer.jar app.jar
RUN sh -c 'touch /app.jar'
ENTRYPOINT ["java","-jar","/app.jar"]