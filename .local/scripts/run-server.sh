#!/bin/bash

# Change directory to project root
SCRIPT_DIR="$(dirname "$0")"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# Load utils
source .local/scripts/utils.sh

log_step "Loading environment configuration..."
ENV_FILE=".env"
load_env "$ENV_FILE"

# Generate solr_auth.txt if SOLR_USER and SOLR_PASSWORD are set
if [ ! -z "$SOLR_USER" ] && [ ! -z "$SOLR_PASSWORD" ]; then
    log_info "Generating solr_auth.txt..."
    echo "httpBasicAuthUser=$SOLR_USER" > solr_auth.txt
    echo "httpBasicAuthPassword=$SOLR_PASSWORD" >> solr_auth.txt
fi

export JAVA_SOLR_OPT="-Dsolr.httpclient.builder.factory=org.apache.solr.client.solrj.impl.PreemptiveBasicAuthClientBuilderFactory -Dsolr.httpclient.config=solr_auth.txt"

# JVM options for Spark 3.4+ and Java 11/17 (mirrors entrypoint.sh)
export JAVA_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

log_success "Starting Cohort Requester Server Mode..."
java $JAVA_OPTS $JAVA_SOLR_OPT -jar target/cohort-requester.jar
