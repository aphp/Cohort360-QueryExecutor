# Cohort360-QueryExecutor Local Helper Repository

This directory contains a refactored and cleaner set of scripts for local development, integrated with the root project's existing Docker configuration.

## Features
- **Integrated**: Uses the root `Dockerfile` and `docker-compose.yml`.
- **Colored Output**: Scripts use a utility logger for better readability.
- **Flexible**: Supports both direct Server Mode (running JAR) and Docker Mode.
- **Isolated**: Keeps local configurations and temporary files out of the main repository's git history.

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Configuration](#configuration)
3. [Setup](#setup)
4. [Running Server Mode (Direct JAR)](#running-server-mode-direct-jar)
5. [Running Docker Mode](#running-docker-mode)
6. [API Usage](#api-usage)

---

## Prerequisites
- **Java 11 or 17** (for Server Mode)
- **Docker and Docker Compose** (for Docker Mode)
- **Maven** (handled via `./mvnw` wrapper)

---

## Configuration
Before running anything, initialize your local environment:

```bash
cp .local/env.example .local/.env
```

Edit `.local/.env` to configure your local environment (FHIR URL, PostgreSQL, Solr, etc.).

---

## Setup
To build the project and download required dependencies (like the PostgreSQL driver):

```bash
./.local/scripts/setup.sh
```

This script:
1. Builds the project using Maven.
2. Downloads the PostgreSQL driver JAR.
3. Automatically initializes `.local/.env` if it doesn't exist.

---

## Running Server Mode (Direct JAR)
Runs the application directly on your host machine. This is faster for iterative development.

```bash
./.local/scripts/run-server.sh
```

This script:
- Loads environment variables from `.local/.env`.
- Generates `solr_auth.txt` for Solr authentication.
- Applies the necessary JVM `--add-opens` flags (matching the production `entrypoint.sh`).

---

## Running Docker Mode
Runs everything in containers.

```bash
./.local/docker/run-docker.sh
```

This script:
- Builds the `sjs:latest` image using the root `Dockerfile`.
- Starts services using the root `docker-compose.yml` with local overrides (`.local/docker/docker-compose.local.yml`).
- Includes a local PostgreSQL container for testing if needed.

---

## API Usage

Once the server is running (port `8091` by default):

### Example Query (Count)
```bash
curl -X POST http://localhost:8091/jobs -H "Content-Type: application/json" -d '{
    "input": {
        "cohortDefinitionSyntax": "{\"sourcePopulation\":{\"caresiteCohortList\":[118]},\"_type\":\"request\",\"request\":{\"_type\":\"andGroup\",\"_id\":0,\"isInclusive\":true,\"criteria\":[{\"_type\":\"basicResource\",\"_id\":1,\"isInclusive\":true,\"resourceType\":\"Patient\",\"filterFhir\":\"active=true&gender=female\",\"criteria\":[],\"dateRangeList\":[],\"temporalConstraints\":[]}],\"dateRangeList\":[],\"temporalConstraints\":[]},\"temporalConstraints\":[]}",
        "mode": "count"
    }
}'
```

### Check Job Status
```bash
curl http://localhost:8091/jobs
```

### Cancel Job
```bash
curl -X DELETE http://localhost:8091/jobs/<jobId>
```
