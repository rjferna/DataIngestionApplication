FROM python:3.12

# Install build dependencies
RUN apt-get update && apt-get install -y \
    g++ \
    build-essential \
    vim \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# If you have a specific Python script or directory, copy it here
COPY archive /app/archive
COPY data /app/data
COPY logs /app/logs
COPY scripts /app/scripts

# Start bash shell in the running container & remain running because of "tail", "-f", "/dev/null"
ENTRYPOINT ["tail", "-f", "/dev/null"]