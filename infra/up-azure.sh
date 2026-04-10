#!/bin/bash
# =============================================================================
# up-azure.sh — Start Airflow for Azure cloud deployment
# -----------------------------------------------------------------------------
# Uses docker-compose.airflow.azure.yml (Dockerfile.azure)
# which includes Databricks + Blob Storage providers instead of Livy.
#
# No postgres, spark modules — those are managed by Azure services:
#   - DB: Neon (external, always available)
#   - Spark: Azure Databricks (DAG creates job clusters on demand)
#   - Storage: Azure Blob (pipeline-data container)
#
# Options:
#   --build      Rebuild Docker images before starting
#   --no-cache   Rebuild Docker images from scratch
#
# Usage:
#   ./up-azure.sh                    # Start Airflow (Azure mode)
#   ./up-azure.sh --build            # Rebuild changed images
#   ./up-azure.sh --no-cache         # Full rebuild from scratch
# =============================================================================

set -e
cd "$(dirname "$0")"

# =============================================================================
# Config Check
# =============================================================================

if [ ! -f .env ]; then
    echo "No .env found — copying config/.env.development"
    cp config/.env.development .env
fi

# =============================================================================
# Parse Arguments
# =============================================================================

BUILD_FLAG=""
NO_CACHE=""

while [ $# -gt 0 ]; do
    case "$1" in
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
            echo "Usage: ./up-azure.sh [--build|--no-cache]"
            echo ""
            echo "Starts Airflow with Azure providers (Databricks + Blob Storage)."
            echo "No local Spark or PostgreSQL — uses Azure managed services."
            exit 0
            ;;
        *)
            echo "Unknown argument: $1"
            echo "Usage: ./up-azure.sh [--build|--no-cache]"
            exit 1
            ;;
    esac
done

# =============================================================================
# Network
# =============================================================================

docker network create giljobi-network 2>/dev/null || true

# =============================================================================
# Host Path Resolution
# =============================================================================

if [ -z "$INFRA_HOST_PATH" ]; then
    export INFRA_HOST_PATH="$(pwd)"
fi

# =============================================================================
# Build (--no-cache)
# =============================================================================

if [ "$NO_CACHE" = "true" ]; then
    echo "Building Azure Airflow image (no cache)..."
    docker compose -p giljobi-airflow -f docker-compose.airflow.azure.yml build --no-cache
fi

# =============================================================================
# Execute
# =============================================================================

echo "=== Giljobi Infrastructure (Azure) ==="
echo "Compose: docker-compose.airflow.azure.yml"
echo "Dockerfile: airflow/Dockerfile.azure"
echo "Config: $(readlink .env 2>/dev/null || echo '.env')"
echo "======================================="

docker compose -p giljobi-airflow -f docker-compose.airflow.azure.yml up -d --wait $BUILD_FLAG
