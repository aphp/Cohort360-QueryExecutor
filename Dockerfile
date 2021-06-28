FROM harbor.eds.aphp.fr/cohort360/openjdk:11-alpine

WORKDIR /app
COPY postgresql.jar postgresql.jar
COPY target/cohort-requester.jar cohort-requester.jar
COPY entrypoint.sh entrypoint.sh

RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
