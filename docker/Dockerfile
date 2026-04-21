FROM python:3.10-slim

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install dependencies
RUN pip install pyspark pandas sqlalchemy psycopg2-binary

WORKDIR /app