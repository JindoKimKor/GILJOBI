# =============================================================================
# infra/airflow/Dockerfile
# -----------------------------------------------------------------------------
# Purpose : Custom Airflow image with Python packages and Docker CLI
# Base    : apache/airflow:2.10.5-python3.12 (CeleryExecutor-ready)
# Added   : Docker Compose CLI, Livy provider, pandas, psycopg2, pyarrow, requests
#
# Used by : airflow-webserver, airflow-scheduler, airflow-worker, airflow-init
#           in docker-compose.airflow.yml
#
# Docker CLI : Allows DAGs to start/stop infrastructure modules (postgres, spark)
#              via up.sh/down.sh. Requires Docker socket mount in compose file.
# =============================================================================

FROM apache/airflow:2.10.5-python3.12

# -----------------------------------------------------------------------------
# 1. Docker Compose CLI (as root)
#    DAGs use BashOperator to call up.sh/down.sh for starting/stopping
#    infrastructure modules (postgres, spark) as part of the pipeline.
#    Requires /var/run/docker.sock mounted in docker-compose.airflow.yml.
# -----------------------------------------------------------------------------
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" > /etc/apt/sources.list.d/docker.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends docker-compose-plugin docker-ce-cli && \
    rm -rf /var/lib/apt/lists/*
USER airflow

# -----------------------------------------------------------------------------
# 2. Python packages
#    IMPORTANT: Pin apache-airflow==${AIRFLOW_VERSION} to prevent pip from
#    accidentally downgrading or removing Airflow during dependency resolution.
#    See: https://github.com/apache/airflow/discussions/53623
#
#    - apache-airflow-providers-apache-livy : LivyOperator for Spark job submission
#    - pandas         : DataFrame operations in market-trend DAG tasks
#    - psycopg2-binary: PostgreSQL connection for DAG tasks (load stage)
#    - pyarrow        : Parquet I/O between transform and load stages
#    - requests       : HTTP client for API calls in DAG tasks
# -----------------------------------------------------------------------------
ARG AIRFLOW_VERSION=2.10.5
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    apache-airflow-providers-apache-livy \
    pandas \
    psycopg2-binary \
    "pyarrow>=23.0" \
    requests
