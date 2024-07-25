FROM eclipse-temurin:11 AS build

COPY . .
RUN ./mvnw clean install -DskipTests
RUN <<EOF
POSTGRES_VERSION=$(./mvnw help:evaluate -Dexpression=postgres.version -q -DforceStdout)
./mvnw org.apache.maven.plugins:maven-dependency-plugin:3.1.1:get -Dartifact=org.postgresql:postgresql:$POSTGRES_VERSION
./mvnw org.apache.maven.plugins:maven-dependency-plugin:3.1.1:copy -Dartifact=org.postgresql:postgresql:$POSTGRES_VERSION -DoutputDirectory=./
mv postgresql-$POSTGRES_VERSION.jar postgresql.jar
EOF

FROM eclipse-temurin:11

RUN useradd -m -s /bin/bash -u 185 spark
USER spark

WORKDIR /app
COPY --from=build postgresql.jar postgresql.jar
COPY --from=build target/cohort-requester.jar cohort-requester.jar
COPY --from=build target/cohort-requester-libs.jar cohort-requester-libs.jar
COPY entrypoint.sh entrypoint.sh

USER root
RUN chmod +x entrypoint.sh
RUN chown -R spark:spark /app

USER spark

ENTRYPOINT ["./entrypoint.sh"]
