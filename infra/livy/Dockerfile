# =============================================================================
# infra/livy/Dockerfile
# -----------------------------------------------------------------------------
# Purpose : Apache Livy REST API server with embedded Spark 3.5.1
# Base    : openeuler/livy:0.8.0-oe2403sp1
# Added   : Spark 3.5.1 + Hadoop 3 (matches spark-master/worker versions)
#
# Used by : livy service in infra/docker-compose.yml
# API     : http://localhost:8998 (Spark job submission from Airflow LivyOperator)
# =============================================================================

FROM openeuler/livy:0.8.0-oe2403sp1

# -----------------------------------------------------------------------------
# 1. PySpark Python paths
#    Both driver and executor must use the same Python version (3.11)
#    to match the Spark worker environment.
# -----------------------------------------------------------------------------
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3.11
ENV PYSPARK_PYTHON=/usr/bin/python3.11

# Switch to root to install Spark
USER root

# -----------------------------------------------------------------------------
# 2. Apache Spark 3.5.1
#    Livy needs a local Spark installation to communicate with the cluster.
#    Version must match spark-master (3.5.1) to avoid compatibility issues.
#    Hadoop 3 binaries are included in the Spark distribution.
# -----------------------------------------------------------------------------
ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

# -----------------------------------------------------------------------------
# 3. Environment variables
#    SPARK_HOME : Livy uses this to locate spark-submit and Spark libraries.
#    PATH       : Adds spark-submit to the system path.
# -----------------------------------------------------------------------------
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
