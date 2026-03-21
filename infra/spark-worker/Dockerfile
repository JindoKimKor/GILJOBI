# =============================================================================
# infra/spark-worker/Dockerfile
# -----------------------------------------------------------------------------
# Purpose : Spark worker image with Python 3.11 for PySpark scripts
# Base    : apache/spark:3.5.1 (matches spark-master image version)
# Added   : Python 3.11 runtime (required by PySpark notebooks)
#
# Used by : spark-worker service in infra/docker-compose.yml
#           Scale with: docker compose up --scale spark-worker=N
# =============================================================================

FROM apache/spark:3.5.1

# Switch to root to install system packages
USER root

# -----------------------------------------------------------------------------
# 1. Python 3.11
#    PySpark scripts in matching-insights require Python 3.11.
#    Using AccelData mirror for Ubuntu packages (compatible with Spark base image).
#    update-alternatives pins python3 → python3.11 as the default.
# -----------------------------------------------------------------------------
RUN wget -qO - https://mirror.odp.acceldata.dev/v2/odp-utils/ubuntu20/my_public.key | apt-key add - && \
    echo "deb [arch=amd64] https://mirror.odp.acceldata.dev/v2/odp-utils/ubuntu20/ ODP main" | tee /etc/apt/sources.list.d/acceldata-staging.list && \
    apt-get update && \
    apt-get install -y python3.11 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 2 && \
    update-alternatives --set python3 /usr/bin/python3.11

# -----------------------------------------------------------------------------
# 2. PySpark Python path
#    Tell PySpark to use python3.11 for worker processes.
# -----------------------------------------------------------------------------
ENV PYSPARK_PYTHON=python3.11
