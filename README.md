# COHORT REQUESTER / SPARK JOB SERVER

The Cohort Requester is a spark application server for querying FHIR data with a set of criteria and return a count or list of patients that match the criteria. 

## Quick Start

Fill in the configuration file `src/main/resources/application.conf` with the appropriate values.
Or use the following minimal needed environment variables:
```bash
# The URL of the FHIR server
export FHIR_URL=http://localhost:8080/fhir
```

Build the project with maven:
```bash
mvn clean package
```

Run the application server:
```bash
java \ 
  --add-opens=java.base/java.lang=ALL-UNNAMED \
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
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  -jar target/cohort-requester.jar
```

Launch a query with the following command:
```bash
curl -X POST http://localhost:8090/jobs -H "Content-Type: application/json" -d '{
    "input": {
        "cohortDefinitionSyntax": "{\"sourcePopulation\":{\"caresiteCohortList\":[118]},\"_type\":\"request\",\"request\":{\"_type\":\"andGroup\",\"_id\":0,\"isInclusive\":true,\"criteria\":[{\"_type\":\"basicResource\",\"_id\":1,\"isInclusive\":true,\"resourceType\":\"Patient\",\"filterFhir\":\"active=true&gender=female\",\"criteria\":[],\"dateRangeList\":[],\"temporalConstraints\":[]},{\"_type\":\"basicResource\",\"_id\":2,\"isInclusive\":true,\"resourceType\":\"Observation\",\"filterFhir\":\"code=http://snomed.info/sct|224299000\",\"criteria\":[],\"occurrence\":{\"n\":1,\"operator\":\">=\"},\"dateRangeList\":[],\"temporalConstraints\":[]}],\"dateRangeList\":[],\"temporalConstraints\":[]},\"temporalConstraints\":[]}",
        "mode": "count"
    }
}'
```

## Job Queries

// TODO : Add a description of the job query format

## Query Format

// TODO : Add a description of the query format

