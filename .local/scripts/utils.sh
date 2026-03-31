#!/bin/bash

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

function log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

function log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

function log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

function log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

function log_step() {
    echo -e "${BLUE}==>${NC} $1"
}

# Function to load environment variables from a file if it exists
function load_env() {
    local env_file="$1"
    if [ -f "$env_file" ]; then
        log_info "Loading environment variables from $env_file..."
        set -a
        source "$env_file"
        set +a
    else
        log_warn "Environment file $env_file not found."
    fi
}
