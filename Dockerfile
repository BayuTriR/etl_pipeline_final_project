# Menggunakan base image resmi Apache Airflow berbasis Python 3.11
FROM apache/airflow:2.9.2-python3.11

# Beralih ke root user untuk menginstal Java
USER root

# Install dependensi untuk mengunduh Java Temurin
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    gnupg \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Install Eclipse Temurin OpenJDK 21
RUN mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /etc/apt/keyrings/adoptium.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME=/ {print $2}' /etc/os-release) main" > /etc/apt/sources.list.d/adoptium.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends temurin-21-jdk && \
    rm -rf /var/lib/apt/lists/*

# Set environment variable untuk JAVA_HOME dan Airflow
ENV JAVA_HOME=/usr/lib/jvm/temurin-21-jdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Download Spark BigQuery connector jar
RUN mkdir -p /opt/spark-jars && \
    curl -L -o /opt/spark-jars/spark-bigquery-with-dependencies_2.12-0.34.0.jar \
    https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.34.0/spark-bigquery-with-dependencies_2.12-0.34.0.jar

# Kembali ke user airflow agar aman secara sekuriti
USER airflow

# Salin requirements.txt
COPY --chown=airflow:root requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY requirements-dbt.txt /requirements-dbt.txt 
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir -r /requirements-dbt.txt

# Salin seluruh file proyek
COPY --chown=airflow:root . .