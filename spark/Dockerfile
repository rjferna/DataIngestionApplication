# Dockerfile in spark_scripts directory
FROM ubuntu:20.04

# Install Java
RUN apt-get update && apt-get install -y default-jdk

# Install Spark
RUN apt-get install -y wget 
RUN wget https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop2.7.tgz 
RUN tar -xvzf spark-3.1.2-bin-hadoop2.7.tgz 
RUN mv spark-3.1.2-bin-hadoop2.7 /usr/local/spark


# Set environment variables
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Copy Spark scripts
COPY data /app/data
COPY logs /app/logs
COPY scripts /app/spark

WORKDIR /app

CMD ["sh", "-c", "python3 /app/scripts/bin/helloworld.py && while true; do sleep 1000; done"]

