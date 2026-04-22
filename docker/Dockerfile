FROM python:3.10-slim

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install dependencies
RUN pip install pyspark pandas sqlalchemy psycopg2-binary flask streamlit requests

WORKDIR /app
