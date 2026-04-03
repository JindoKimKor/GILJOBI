# =============================================================================
# infra/spark-worker-sd/Dockerfile
# -----------------------------------------------------------------------------
# Purpose : Spark worker image for skill-demand pipeline
# Base    : apache/spark:3.5.1
# Added   : Python 3.11 + numpy, pandas, sentence-transformers, psycopg2
#
# Used by : spark-worker-sd service in docker-compose.spark-sd.yml
# =============================================================================

FROM apache/spark:3.5.1

USER root

# -----------------------------------------------------------------------------
# 1. Python 3.11
# -----------------------------------------------------------------------------
RUN wget -qO - https://mirror.odp.acceldata.dev/v2/odp-utils/ubuntu20/my_public.key | apt-key add - && \
    echo "deb [arch=amd64] https://mirror.odp.acceldata.dev/v2/odp-utils/ubuntu20/ ODP main" | tee /etc/apt/sources.list.d/acceldata-staging.list && \
    apt-get update && \
    apt-get install -y python3.11 python3.11-distutils curl && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 2 && \
    update-alternatives --set python3 /usr/bin/python3.11 && \
    rm -rf /var/lib/apt/lists/*

# -----------------------------------------------------------------------------
# 1b. Install pip for Python 3.11 (system pip doesn't match 3.11)
# -----------------------------------------------------------------------------
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11

ENV PYSPARK_PYTHON=python3.11

# -----------------------------------------------------------------------------
# 2. Python packages
#    - numpy, pandas, pyarrow: data processing + parquet I/O
#    - sentence-transformers: NOC title semantic matching (Step 2)
#    - psycopg2-binary: PostgreSQL connection (NOC titles lookup)
# -----------------------------------------------------------------------------
RUN python3.11 -m pip install --no-cache-dir \
        numpy pandas pyarrow \
        sentence-transformers \
        psycopg2-binary sqlalchemy

# -----------------------------------------------------------------------------
# 3. Node.js + Claude CLI (for Step 3+4 LLM calls)
#    Claude CLI for LLM-based skill extraction (Step 3+4)
# -----------------------------------------------------------------------------
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    rm -rf /var/lib/apt/lists/*

RUN npm install -g @anthropic-ai/claude-code --unsafe-perm

RUN node --version && npm --version && claude --version
