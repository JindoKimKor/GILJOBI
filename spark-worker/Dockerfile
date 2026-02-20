FROM apache/spark:3.5.1
USER root

RUN wget -qO - https://mirror.odp.acceldata.dev/v2/odp-utils/ubuntu20/my_public.key | apt-key add - && \
    echo "deb [arch=amd64] https://mirror.odp.acceldata.dev/v2/odp-utils/ubuntu20/ ODP main" | tee /etc/apt/sources.list.d/acceldata-staging.list && \
    apt-get update && \
    apt-get install -y python3.11 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 2 && \
    update-alternatives --set python3 /usr/bin/python3.11

ENV PYSPARK_PYTHON=python3.11