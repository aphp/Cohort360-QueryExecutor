# COHORT REQUESTER / SPARK JOB SERVER

[![Actions Status](https://github.com/aphp/Cohort360-QueryExecutor/workflows/build/badge.svg)](https://github.com/aphp/Cohort360-QueryExecutor/actions)
[![Coverage Status](https://sonarcloud.io/api/project_badges/measure?project=aphp_Cohort360-QueryExecutor&metric=coverage)](https://sonarcloud.io/component_measures?id=aphp_Cohort360-QueryExecutor&metric=coverage)
[![Quality Gate](https://sonarcloud.io/api/project_badges/measure?project=aphp_Cohort360-QueryExecutor&metric=alert_status)](https://sonarcloud.io/dashboard?id=aphp_Cohort360-QueryExecutor)

[![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)

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

The job query format is as follows :
```json
{
    "input": {
        "cohortDefinitionSyntax": "<cohort definition syntax>",
        "mode": "<mode>",
        "modeOptions": { // optional mode options
          // optional list of criteria ids separated by commas or "all", this will activate a detailed count of the patients per criteria
          // or "ratio", this will activate a detailed count of final matched patients per criteria
           "details": "<details>",
          // optional sampling ratio value between 0.0 and 1.0 to limit the number of patients of the cohort to create (it can be used to sample an existing cohort)
           "sampling": "<sampling>",
          // optional cohort id to use as a base for the "createDiff" mode
           "baseCohortId": "<base cohort id>"
        },
        "callbackUrl": "<callback url>" // optional callback url to retrieve the result
    }
}
```

with `mode` being one of the following values:
- `count` : Return the number of patients that match the criteria of the `cohortDefinitionSyntax`
- `count_all`: Return the number of patients that match the criteria of the `cohortDefinitionSyntax` and an estimation minimum and maximum bounds
- `count_with_details`: Return the number of patients that match the criteria of the `cohortDefinitionSyntax` with a detailed count per organization
- `create`: Create a cohort of patients that match the criteria of the `cohortDefinitionSyntax`
- `create_diff`: Create a change list from a base cohort of patients (defined in `modeOptions`) and the new/deleted ones that match the criteria of the `cohortDefinitionSyntax`

and `cohortDefinitionSyntax` being a JSON string that represents the criteria described in the [query format section](#query-format).

### Job Response

If a `callbackUrl` is provided (else the result will be printed in the logs), the response will be sent to the provided URL with the following format:
```typescript
type RESPONSE = {
    _type: string; // the job mode (count or create)
    message: string; // the message of the response (usually the job status)
    groupId: string; // the cohort id in case of a cohort creation job
    count: number; // the count of patients in case of a count job
    extra: Map<string, string>; // extra information about the count job (like details count per organization or per criteria)
    stack: string; // the stack trace in case of an error
}
```

### Query Format

The query format of the `cohortDefinitionSyntax` field is as follows (described in `typescript`):

```typescript
type REQUEST = {
    sourcePopulation: {
        caresiteCohortList?: Array<Integer> // Optional list of caresite ids that will be used to filter the resource with the fhir param `_list` 
    },
    _type: "request",
    request: CRITERIA,
    resourceType?: string // The fhir resource type of the cohort (Patient being the default)
} 

type CRITERIA = {
    _type: "andGroup" | "orGroup" | "nAmongM" | "basicResource",
    _id: Integer, // Unique identifier of the criteria
    isInclusive: Boolean // If true, the criteria is inclusive, if false, the criteria is exclusive
}

type GROUP_CRITERIA = CRITERIA & {
    _type: "andGroup" | "orGroup" | "nAmongM", // the type of the group "and" or "or" or "at least n matches among subcriterias"
    criteria: Array<CRITERIA>, // list of subcriterias (can be basicResource or group)
    temporalConstraints: Array<TEMPORAL_CONSTRAINT>, // list of temporal constraints between the subcriterias
    nAmongMOptions: Array<OCCURENCE_CONSTRAINT> // list of occurrence constraints for the nAmongM type
}

type BASIC_RESOURCE = CRITERIA & {
    _type: "basicResource",
    resourceType: string, // The fhir resource type to filter
    filterFhir: string, // The fhir query string to filter the resource
    filterSolr?: string, // The solr query string to filter the resource (if using solr resolver)
    occurrence?: OCCURENCE_CONSTRAINT, // The occurrence constraint of the resource
    patientAge?: PATIENT_AGE, // The age constraint of the patient
    dateRangeList?: Array<DATE_RANGE>, // The date range constraint of the resource
    encounterDateRange?: DATE_RANGE // The date range constraint of the related encounter
    uniqueFields?: Array<UNIQUE_FIELD> // The unique fields of the resource to count the patients
}

type PATIENT_AGE = {
    minAge?: string, // with the format {year}-{month}-{day}, eg. for someone of 21 years old this would be 21-0-0
    maxAge?: string, // with the format {year}-{month}-{day}, eg. for someone of 21 years old this would be 21-0-0
    datePreference?: string[], // resource reference field of the date to calculate the age
    dateIsNotNull?: boolean
}

type DATE_RANGE = {
    minDate?: string, // with the standard format {year}-{month}-{day}, eg. for 2024-01-25
    maxDate?: string, // with the standard format {year}-{month}-{day}, eg. for 2024-01-25
    datePreference?: string[], // resource reference field of the date to filter
    dateIsNotNull?: boolean
}

type UNIQUE_FIELD = {
    name?: string, // The fhir resource field to verify, for now only codes are supported and it is not need to fill this field which is ignored
    operator: string, // The operator to use for the verification
    n: number // The number of unique values
}

type TEMPORAL_CONSTRAINT = {
    idList: string | Array<number>,
    constraintType: "sameEncounter" | "differentEncounter" | "directChronologicalOrdering" | "sameEpisodeOfCare",
    occurrenceChoiceList?: Array<{_id: number, occurrenceChoice: string}>,
    timeRelationMinDuration?: TEMPORAL_CONSTRAINT_DURATION,
    timeRelationMaxDuration?: TEMPORAL_CONSTRAINT_DURATION,
    datePreferenceList?: Array<{_id: number, occurrenceChoice: string}>,
    filteredCriteriaIdList?: string | Array<number>,
    dateIsNotNullList?: boolean | {_id: number, dateIsNotNull: boolean}
}

type TEMPORAL_CONSTRAINT_DURATION = {
    years?: number,
    months?: number,
    days?: number,
    hours?: number,
    minutes?: number,
    seconds?: number
}

type OCCURENCE_CONSTRAINT = {
    n: number,
    operator: string,
    sameEncounter?: boolean,
    sameDay?: boolean,
    timeDelayMin?: boolean,
    timeDelayMax?: boolean
}
```

