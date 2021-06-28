#!/bin/sh
# enable OMOP pg access
echo "$PG_HOST:$PG_PORT:$PG_DB:$PG_USER:$DB_OMOP_PASSWORD" >> /root/.pgpass
echo "$SJS_PG_HOST:$SJS_PG_PORT:$SJS_PG_DB:$SJS_PG_USER:$SJS_PG_PASSWORD" >> /root/.pgpass
chmod 0600 /root/.pgpass

# load env var
#envsubst < env.conf > tmp_env.conf && mv tmp_env.conf env.conf

# enable solr access
#envsubst < solr_auth.txt > tmp_solr_auth.txt && mv tmp_solr_auth.txt solr_auth.txt
echo -e "httpBasicAuthUser=$SOLR_USER\nhttpBasicAuthPassword=$SOLR_PASSWORD" > solr_auth.txt

# additional driver JVM option
#envsubst < log4j-jobserver.properties > tmp.properties && mv tmp.properties log4j-jobserver.properties
#export LOG_OPT="-Dlog4j.configuration=file:log4j-jobserver.properties"
export JAVA_SOLR_OPT="-Dsolr.httpclient.builder.factory=org.apache.solr.client.solrj.impl.PreemptiveBasicAuthClientBuilderFactory -Dsolr.httpclient.config=solr_auth.txt"

echo $LOG_OPT
# start server

export JAVA_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
java $JAVA_OPTS $JAVA_SOLR_OPT -jar cohort-requester.jar