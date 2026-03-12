# livy/Dockerfile
FROM openeuler/livy:0.8.0-oe2403sp1

ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3.11
ENV PYSPARK_PYTHON=/usr/bin/python3.11

USER root

# apache/spark:latest 와 동일한 Spark 버전 설치 (3.5.x)
ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH