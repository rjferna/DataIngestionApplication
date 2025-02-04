# Dockerfile in spark_scripts directory
FROM ubuntu:20.04

# Set the environment variable to avoid interactive prompts when building image
ENV DEBIAN_FRONTEND=noninteractive

# Install vim, python3, pip3 and Java
RUN apt-get update && apt-get install -y \ 
    vim \
    default-jdk \
    python3 \
    python3-pip

# Install Spark
RUN apt-get install -y wget 
RUN wget https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop2.7.tgz 
RUN tar -xvzf spark-3.1.2-bin-hadoop2.7.tgz 
RUN mv spark-3.1.2-bin-hadoop2.7 /usr/local/spark

# Install PostgreSQL JDBC driver
RUN wget https://jdbc.postgresql.org/download/postgresql-42.3.1.jar -P /usr/local/spark/jars

# Set environment variables
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Copy Spark scripts
COPY data /app/data
COPY logs /app/logs
COPY scripts /app/scripts

WORKDIR /app

# Start bash shell in the running container & remain running because of "tail", "-f", "/dev/null"
ENTRYPOINT ["tail", "-f", "/dev/null"]

#CMD ["sh", "-c", "python3 /app/scripts/bin/helloworld.py && while true; do sleep 1000; done"]