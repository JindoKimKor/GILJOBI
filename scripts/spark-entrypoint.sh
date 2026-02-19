#!/bin/bash

# Download the PostgreSQL JDBC driver if it doesn't exist
JDBC_JAR="/opt/spark/jars/custom/postgresql-42.7.1.jar"
JDBC_URL="https://jdbc.postgresql.org/download/postgresql-42.7.1.jar"

if [ ! -f "$JDBC_JAR" ]; then
    echo "Downloading PostgreSQL JDBC driver..."
    mkdir -p /opt/spark/jars/custom
    curl -L -o "$JDBC_JAR" "$JDBC_URL"
    echo "✓ PostgreSQL JDBC driver downloaded"
else
    echo "✓ PostgreSQL JDBC driver already exists"
fi

exec "$@"