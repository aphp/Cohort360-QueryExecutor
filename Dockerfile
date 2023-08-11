FROM harbor.eds.aphp.fr/cohort360/openjdk:11

RUN useradd -m -s /bin/bash -u 185 spark
USER spark

WORKDIR /app
COPY postgresql.jar postgresql.jar
COPY target/cohort-requester.jar cohort-requester.jar
COPY target/cohort-requester-libs.jar cohort-requester-libs.jar
COPY entrypoint.sh entrypoint.sh

USER root
RUN chmod +x entrypoint.sh
RUN chown -R spark:spark /app

USER spark

ENTRYPOINT ["./entrypoint.sh"]
