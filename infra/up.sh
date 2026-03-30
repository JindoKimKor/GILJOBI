#!/bin/bash
# =============================================================================
# up.sh — Start infrastructure modules
# -----------------------------------------------------------------------------
# Selects which modules to start. All configuration (replicas, memory, cores)
# is read from .env, which is symlinked from config/.env.{environment}.
#
# Modules:
#   postgres  — Pipeline data DB (local development)
#   airflow   — Airflow + Redis + Airflow-DB (orchestration)
#   spark     — Spark + Livy (distributed processing)
#
# Options:
#   --build      Rebuild Docker images before starting
#   --no-cache   Rebuild Docker images from scratch
#
# Usage:
#   ./up.sh airflow                    # Orchestration only (DAGs manage infra)
#   ./up.sh postgres                   # Pipeline DB only (manual py main.py)
#   ./up.sh airflow spark              # Orchestration + distributed processing
#   ./up.sh airflow --build            # Rebuild changed images
#   ./up.sh airflow --no-cache         # Full rebuild from scratch
#
# Configuration:
#   All settings (replicas, memory, cores, credentials) are in config/:
#     config/.env.development   — Local development defaults
#     config/.env.production    — Production settings
#     config/.env.example       — Template
#
#   Active config is symlinked to .env:
#     ln -sf config/.env.development .env
#
#   Docker Compose reads .env automatically.
# =============================================================================

set -e
cd "$(dirname "$0")"

# =============================================================================
# Config Check
# -----------------------------------------------------------------------------
# Ensure .env exists. If not, create symlink to development config.
# =============================================================================

if [ ! -f .env ]; then
    echo "No .env found — copying config/.env.development"
    cp config/.env.development .env
fi

# =============================================================================
# Parse Arguments
# -----------------------------------------------------------------------------
# Only modules and build flags. No --workers, --memory, --cores.
# Those are all in .env now.
# =============================================================================

MODULES=""
BUILD_FLAG=""
NO_CACHE=""

show_usage() {
    echo "Usage: ./up.sh <modules...> [--build|--no-cache]"
    echo ""
    echo "Modules:"
    echo "  postgres       Pipeline data DB (market-trend)"
    echo "  postgres-sd    Skill-demand DB (separate)"
    echo "  airflow        Airflow + Redis + metadata DB"
    echo "  spark          Spark + Livy"
    echo ""
    echo "Options:"
    echo "  --build      Rebuild images (use cached layers)"
    echo "  --no-cache   Rebuild images from scratch"
    echo ""
    echo "Config: edit config/.env.{environment}, symlink to .env"
    echo "  ln -sf config/.env.development .env"
    echo "  ln -sf config/.env.production .env"
    echo ""
    echo "Examples:"
    echo "  ./up.sh airflow                # Start Airflow (DAGs manage rest)"
    echo "  ./up.sh postgres airflow       # DB + Airflow"
    echo "  ./up.sh airflow spark          # Airflow + Spark"
    echo "  ./up.sh airflow --no-cache     # Rebuild from scratch"
    exit 1
}

if [ $# -eq 0 ]; then
    show_usage
fi

while [ $# -gt 0 ]; do
    case "$1" in
        postgres|postgres-sd|airflow|spark)
            MODULES="$MODULES $1"
            shift
            ;;
        --build)
            BUILD_FLAG="--build"
            shift
            ;;
        --no-cache)
            NO_CACHE="true"
            BUILD_FLAG="--build"
            shift
            ;;
        -h|--help)
            show_usage
            ;;
        *)
            echo "Unknown argument: $1"
            show_usage
            ;;
    esac
done

if [ -z "$MODULES" ]; then
    echo "Error: No modules specified."
    show_usage
fi

# =============================================================================
# Network
# -----------------------------------------------------------------------------
# Shared Docker network for cross-module communication.
# Created once, ignored if already exists.
# =============================================================================

docker network create giljobi-network 2>/dev/null || true

# =============================================================================
# Host Path Resolution
# -----------------------------------------------------------------------------
# When running inside a Docker container (e.g., Airflow worker calling up.sh),
# volume mounts must use HOST paths, not container paths.
# INFRA_HOST_PATH is passed to Airflow containers at startup.
# When running from the host, resolve it from pwd.
# =============================================================================

if [ -z "$INFRA_HOST_PATH" ]; then
    export INFRA_HOST_PATH="$(pwd)"
fi

# =============================================================================
# Build (--no-cache)
# -----------------------------------------------------------------------------
# If --no-cache, run docker compose build --no-cache before up.
# =============================================================================

if [ "$NO_CACHE" = "true" ]; then
    echo "Building images (no cache)..."
    for MODULE in $MODULES; do
        case "$MODULE" in
            postgres)    docker compose -p market-trend-db -f docker-compose.postgres.yml build --no-cache ;;
            postgres-sd) docker compose -p skill-demand-db -f docker-compose.postgres-sd.yml build --no-cache ;;
            airflow)     docker compose -p giljobi-airflow -f docker-compose.airflow.yml build --no-cache ;;
            spark)       docker compose -p giljobi-spark -f docker-compose.spark.yml build --no-cache ;;
        esac
    done
fi

# =============================================================================
# Execute
# -----------------------------------------------------------------------------
# Start each module with its own project name for Docker Desktop grouping:
#   📁 giljobi-airflow       (airflow-db, redis, webserver, scheduler, worker)
#   📁 market-trend-db       (postgres — market-trend)
#   📁 skill-demand-db       (postgres — skill-demand)
#   📁 giljobi-spark         (spark-master, spark-worker, livy)
# =============================================================================

echo "=== Giljobi Infrastructure ==="
echo "Modules:$MODULES"
echo "Config: $(readlink .env 2>/dev/null || echo '.env')"
echo "=============================="

for MODULE in $MODULES; do
    case "$MODULE" in
        postgres)    docker compose -p market-trend-db -f docker-compose.postgres.yml up -d --wait $BUILD_FLAG ;;
        postgres-sd) docker compose -p skill-demand-db -f docker-compose.postgres-sd.yml up -d --wait $BUILD_FLAG ;;
        airflow)     docker compose -p giljobi-airflow -f docker-compose.airflow.yml up -d --wait $BUILD_FLAG ;;
        spark)       docker compose -p giljobi-spark -f docker-compose.spark.yml up -d --wait $BUILD_FLAG ;;
    esac
done
