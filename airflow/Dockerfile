# Use the official Apache Airflow image as the base
FROM apache/airflow:2.8.1

# Switch to root user to install dependencies
USER root

# Install additional system dependencies (if needed for Python or Spark interaction)
RUN apt-get update && apt-get install -y \
    vim \
    curl \
    python3.12 \
    python3-pip \    
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

# Install Python dependencies for interacting with Postgres and Docker
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy custom Airflow configuration (optional, if you want to override defaults)
# COPY airflow.cfg /app/airflow/airflow.cfg

COPY data /app/data
COPY logs /app/logs
COPY scripts /app/scripts

# Set working directory for DAGs
WORKDIR /app