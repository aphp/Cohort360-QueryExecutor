# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the Cohort Requester (Spark Job Server) - a Scala/Spark application that executes FHIR queries to identify patient cohorts. It serves as the query execution engine for the Cohort360 project, processing complex medical data queries with temporal constraints and returning patient counts or cohort lists.

The full job/query request format (modes, `cohortDefinitionSyntax`, temporal constraints, etc.) is documented in `README.md` — read it before touching query parsing or the public API.

## Build and Development Commands

Use the `./mvnw` wrapper (Maven). Java 11 or 17, Scala 2.12.15, Spark 3.4.0.

```bash
./mvnw clean compile          # Compile main source
./mvnw clean package          # Build + run tests
./mvnw package -DskipTests    # Build without tests
./mvnw clean verify           # Full verification incl. tests
```

The package phase produces two JARs: `target/cohort-requester.jar` (shaded app, Spark scoped `provided`) and `target/cohort-requester-libs.jar`.

### Testing

```bash
./mvnw test                                   # All tests
./mvnw test -Dtest=ClassName                  # Single test class
./mvnw test -Dtest=ClassName#testMethodName   # Single test method
```

- Tests use **ScalaTest** (`AnyFunSuite` / `AnyFunSuiteLike`) with Mockito; `spark-fast-tests` for DataFrame assertions.
- Test config: `src/test/resources/application.test.conf`.
- Tests needing Spark mix in a Spark-session-providing base class.
- Many tests are **data-driven**: each subdirectory under `src/test/resources/testCases/` (and `resolver/restfhir/testCases/`) is a scenario (request JSON + expected result). Add a new scenario by adding a directory there rather than hardcoding cases in Scala.

### Running locally

The app needs a long list of `--add-opens` JVM flags for Spark to access internal Java APIs. Don't hand-maintain them — copy the exact invocation from `entrypoint.sh` or the README, or run via `docker-compose up` (multi-stage `Dockerfile`).

### Code style

Scalafmt with `maxColumn = 100` (`.scalafmt.conf`).

## Architecture Overview

### Request → result flow

1. **`server/`** (Scalatra/Jetty, port 8090) — `JobController` exposes `POST/GET/DELETE /jobs`. It deserializes the request into a `SparkJobParameter` and hands it to the `JobManager`.
2. **`jobs/JobManager`** — runs each job asynchronously on a fixed thread pool (`app.jobs.threads`) via `Future`, tracks it by a generated UUID in an in-memory `TrieMap`, sets a Spark job group (for fair scheduling + cancellation), supports `autoRetry`, and on completion POSTs a callback (see `callbackUrl` resolution in `JobBase`/`CountQuery`). Results are also kept in memory for `GET /jobs/:id`.
3. **`JobBase` implementations** — `CountQuery` and `CreateQuery` (in the root package) are the two job types. They build the query plan and execute it on Spark.
4. **`query/engine/QueryBuilder`** — `DefaultQueryBuilder.processRequest` turns a parsed `Request` into a Spark `DataFrame` of matching subjects. It delegates recursively through `QueryBuilderGroup` (AND/OR/nAmongM logic + temporal constraints) down to `QueryBuilderBasicResource` (single FHIR resource criteria).
5. **`query/resolver/ResourceResolver`** — the basic-resource builder fetches each resource's data as a DataFrame through a resolver (see below).

`CountQuery` short-circuits: a request with no real criteria counts directly via the resolver (`countPatients`); otherwise it runs the full Spark plan and `.count()`s the result.

### Pluggable resolvers and cohort creation (key extension points)

These are factory-dispatched abstractions — the main places to extend when adding a backend:

- **`query/resolver/ResourceResolver`** (trait) — fetches FHIR resources as Spark DataFrames. Implementations: `solr/SolrQueryResolver`, `rest/RestFhirResolver`. Selected by `ResourceResolver.get(ResourceResolvers.{solr,fhir})`; default is `app.defaultResolver` (default `solr`).
- **`cohort/CohortCreation`** (trait) — persists created cohorts. Implementations: `fhir/FhirCohortCreation` (writes FHIR `List`/`Group` resources), `pg/PGCohortCreation` (writes cohort + items tables via `PGTool`). Selected by `CohortCreation.get(CohortCreationServices.{fhir,pg})`; default is `app.defaultCohortCreationService` (default `pg`).

Each has its own `*QueryElementsConfig` describing the searchable/joinable fields per FHIR resource type.

### DataFrame column conventions

The `requester` package object (`requester.scala`) defines the string constants that flow through every DataFrame transformation. Internal query columns use `QueryColumn` (`patient`, `encounter`, `event_date`, `organizations`, …); final output columns use `ResultColumn` (`subject_id`, `organization_ids`). FHIR resource type names and Solr collection names are also constants here. Prefer these constants over string literals.

### Configuration

- Main config: `src/main/resources/application.conf` (HOCON). Loaded once into `AppConfig` (singleton `AppConfig.get`).
- Almost every value is overridable by an env var using the `setting = ${?ENV_VAR}` pattern (e.g. `DEFAULT_RESOLVER`, `JOBS_THREADS`, `PG_HOST`, `SOLR_ZK`, `FHIR_URL`). `solr`/`fhir`/`postgres` sections become `Option`s — absent config means that backend is unavailable.
- Solr auth is read from a `solr_auth.txt` file at runtime (path from `solr.authFile`).
- Logging via `LazyLogging`/Log4j (`src/main/resources/log4j.properties`).

### Query types (job `mode`)

`count`, `count_all` (count + randomized min/max bounds), `count_with_details` (count per organization), `create` (build + persist a cohort), `create_diff` (diff against a base cohort). Full semantics in `README.md`.

### Temporal constraints

Sophisticated relationships handled in `QueryBuilderTemporalConstraint`: `sameEncounter`, `differentEncounter`, `directChronologicalOrdering`, `sameEpisodeOfCare`, plus per-resource date-range filtering with date-preference fallbacks.

## Conventions

- Commit messages follow Conventional Commits (`.commitlintrc.yaml`, `cliff.toml`); the changelog is generated. Releases go through `scripts/createReleaseCommit.sh`.
