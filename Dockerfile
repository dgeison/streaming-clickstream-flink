FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk curl \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH=/app

WORKDIR /app

RUN mkdir -p /app/lib && \
    curl -fsSL -o /app/lib/flink-sql-connector-kafka.jar \
      https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar && \
    curl -fsSL -o /app/lib/flink-connector-jdbc.jar \
      https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.4.0-1.20/flink-connector-jdbc-3.4.0-1.20.jar && \
    curl -fsSL -o /app/lib/postgresql-driver.jar \
      https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
