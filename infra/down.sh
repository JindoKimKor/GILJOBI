#!/bin/bash
# =============================================================================
# down.sh — Stop infrastructure modules
# -----------------------------------------------------------------------------
# Stop and remove containers for specified modules.
# Use -v flag to also remove volumes (full reset).
#
# Usage:
#   ./down.sh                    # Stop all running modules
#   ./down.sh spark              # Stop Spark only
#   ./down.sh airflow spark      # Stop Airflow + Spark
#   ./down.sh -v                 # Stop all + remove volumes (full reset)
#   ./down.sh postgres -v        # Stop PostgreSQL + delete data volume
# =============================================================================

set -e
cd "$(dirname "$0")"

# =============================================================================
# Parse Arguments
# =============================================================================

MODULES=""
EXTRA_ARGS=""

for arg in "$@"; do
    case "$arg" in
        postgres)    MODULES="$MODULES postgres" ;;
        postgres-sd) MODULES="$MODULES postgres-sd" ;;
        airflow)     MODULES="$MODULES airflow" ;;
        spark)       MODULES="$MODULES spark" ;;
        -v|--volumes) EXTRA_ARGS="$EXTRA_ARGS -v" ;;
        -h|--help)
            echo "Usage: ./down.sh [modules...] [-v]"
            echo ""
            echo "Modules: postgres, postgres-sd, airflow, spark"
            echo "  -v    Remove volumes (full reset, deletes data)"
            echo ""
            echo "Examples:"
            echo "  ./down.sh                    # Stop all"
            echo "  ./down.sh spark              # Stop Spark only"
            echo "  ./down.sh -v                 # Stop all + delete volumes"
            exit 0
            ;;
        *)
            echo "Unknown argument: $arg"
            exit 1
            ;;
    esac
done

# If no modules specified, stop all
if [ -z "$MODULES" ]; then
    MODULES="postgres postgres-sd airflow spark"
    echo "Stopping all modules..."
else
    echo "Stopping: $MODULES"
fi

# =============================================================================
# Execute
# -----------------------------------------------------------------------------
# Each module uses its own project name (matching up.sh) so Docker knows
# which containers belong to which module.
# =============================================================================

for MODULE in $MODULES; do
    echo "--- $MODULE ---"
    case "$MODULE" in
        postgres)
            docker compose -p market-trend-db -f docker-compose.postgres.yml down $EXTRA_ARGS 2>&1 || true
            ;;
        postgres-sd)
            docker compose -p skill-demand-db -f docker-compose.postgres-sd.yml down $EXTRA_ARGS 2>&1 || true
            ;;
        airflow)
            docker compose -p giljobi-airflow -f docker-compose.airflow.yml down $EXTRA_ARGS 2>&1 || true
            ;;
        spark)
            docker compose -p giljobi-spark -f docker-compose.spark.yml down $EXTRA_ARGS 2>&1 || true
            ;;
    esac
done
