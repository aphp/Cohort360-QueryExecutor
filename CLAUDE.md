# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the Cohort Requester (Spark Job Server) - a Scala/Spark application that executes FHIR queries to identify patient cohorts. It serves as the query execution engine for the Cohort360 project, processing complex medical data queries with temporal constraints and returning patient counts or cohort lists.

## Build and Development Commands

### Build
```bash
# Clean and build the project
mvn clean package

# Build without tests
mvn clean compile

# Build with Maven wrapper
./mvnw clean package
```

### Testing
```bash
# Run tests
mvn test

# Run tests with Maven wrapper  
./mvnw test

# Clean and run full verification (includes tests)
mvn clean verify
./mvnw clean verify
```

### Run Application
```bash
# Build and run locally
mvn clean package
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

### Code Quality
```bash
# Run SonarCloud analysis (requires tokens)
mvn verify sonar:sonar \
  -Dsonar.sources=src/main/scala \
  -Dsonar.tests=src/test/scala \
  -Dsonar.scala.coverage.reportPaths=target/scoverage.xml
```

## Architecture Overview

### Core Components

1. **Application Server** (`Application.scala`)
   - Entry point using Jetty and Scalatra
   - REST API server on port 8090 (configurable)

2. **Job Management** (`jobs/`)
   - `JobManager`: Handles job execution with thread pools
   - `JobBase`: Base class for all job types
   - Supports concurrent job execution with fair scheduling

3. **Query Engine** (`query/`)
   - `QueryBuilder`: Core query processing logic
   - `QueryParser`: Parses cohort definition syntax (JSON)
   - Supports complex FHIR queries with temporal constraints
   - Handles logical operations (AND/OR groups, N-among-M)

4. **Data Resolvers** (`query/resolver/`)
   - **FHIR REST Resolver**: Queries FHIR servers directly
   - **Solr Resolver**: Queries indexed FHIR data via Solr
   - **PostgreSQL Resolver**: Direct database queries

5. **Cohort Creation** (`cohort/`)
   - **FHIR Cohort Creation**: Creates FHIR Cohort resources
   - **PostgreSQL Cohort Creation**: Stores cohorts in database tables

### Configuration

- Main config: `src/main/resources/application.conf`
- Environment-driven configuration with fallbacks
- Key settings: FHIR URL, PostgreSQL connection, Solr ZooKeeper, Spark settings

### Query Types

- **Count queries**: Return patient counts matching criteria
- **Create queries**: Generate patient cohorts and store them
- **Create diff queries**: Compare cohorts and track changes

### Temporal Constraints

Supports sophisticated temporal relationships:
- Same encounter constraints
- Chronological ordering
- Episode of care relationships
- Date range filtering with multiple date preference options

## Development Notes

- **Language**: Scala 2.12.15 with Spark 3.4.0
- **Testing**: ScalaTest with Mockito for mocking
- **Java Version**: Java 11 (required for Spark compatibility)
- **Packaging**: Creates both shaded JAR and Spring Boot executable JAR

### Key Dependencies

- Apache Spark for distributed processing
- HAPI FHIR for FHIR resource handling
- Scalatra for REST API
- PostgreSQL driver for database operations
- Apache Solr for search indexing

### Test Structure

Tests located in `src/test/scala/` mirror main source structure. Includes comprehensive test cases in `src/test/resources/testCases/` covering various query scenarios like temporal constraints, exclusions, and edge cases.