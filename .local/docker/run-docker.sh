#!/bin/bash

# Change directory to project root
SCRIPT_DIR="$(dirname "$0")"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# Load utils
source .local/scripts/utils.sh

log_step "Checking environment configuration..."
ENV_FILE=".env"

if [ ! -f "$ENV_FILE" ]; then
    log_warn "$ENV_FILE file not found. Creating it from .local/env.example..."
    cp .local/env.example "$ENV_FILE"
fi

log_step "Starting Docker Compose services..."
sudo docker compose -f docker-compose.yml -f .local/docker/docker-compose.local.yml up --build
