#!/bin/bash
set -e

# Change directory to project root
SCRIPT_DIR="$(dirname "$0")"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# Load utils
source .local/scripts/utils.sh

log_step "Starting project setup..."

# Check for .env file
if [ ! -f ".local/.env" ]; then
    log_warn ".local/.env file not found. Creating it from .local/env.example..."
    cp .local/env.example .local/.env
    log_info "Please review and edit .local/.env as needed."
fi

log_step "Building the project..."
if ./mvnw clean package -DskipTests; then
    log_success "Project built successfully."
else
    log_error "Build failed."
    exit 1
fi

log_step "Preparing dependencies..."
mkdir -p target

# Fetch PostgreSQL JAR if not present
POSTGRES_VERSION=$(./mvnw help:evaluate -Dexpression=postgres.version -q -DforceStdout)
if [ ! -f "postgresql.jar" ]; then
    log_info "Downloading PostgreSQL driver (version $POSTGRES_VERSION)..."
    ./mvnw org.apache.maven.plugins:maven-dependency-plugin:3.1.1:get -Dartifact=org.postgresql:postgresql:$POSTGRES_VERSION
    ./mvnw org.apache.maven.plugins:maven-dependency-plugin:3.1.1:copy -Dartifact=org.postgresql:postgresql:$POSTGRES_VERSION -DoutputDirectory=./
    mv postgresql-$POSTGRES_VERSION.jar postgresql.jar
    log_success "PostgreSQL driver downloaded."
else
    log_info "PostgreSQL driver already exists."
fi

log_success "Setup complete!"
