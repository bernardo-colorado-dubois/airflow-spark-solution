# syntax=docker/dockerfile:1
ARG AIRFLOW_VERSION=3.3.1
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG SPARK_VERSION=4.1.3
ARG SPARK_HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
    JAVA_HOME=/usr/lib/jvm/temurin-17-jdk-amd64

USER root

# ---- OS deps: JDK 17 (Spark 4.x baseline) + curl/tar for the Spark tarball
RUN apt-get update -qq \
    && apt-get install -y --no-install-recommends \
       wget curl gnupg2 ca-certificates procps \
    && mkdir -p /etc/apt/keyrings \
    && wget -q -O /etc/apt/keyrings/adoptium.asc https://packages.adoptium.net/artifactory/api/gpg/key/public \
    && echo "deb [signed-by=/etc/apt/keyrings/adoptium.asc] https://packages.adoptium.net/artifactory/deb $(awk -F= '/VERSION_CODENAME/{print $2}' /etc/os-release) main" \
       > /etc/apt/sources.list.d/adoptium.list \
    && apt-get update -qq \
    && apt-get install -y --no-install-recommends temurin-17-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ---- Spark client binaries (gives us spark-submit / spark-shell locally,
#      used by SparkSubmitOperator to talk to the spark-master service)
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz" -O /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION} /opt/spark \
    && rm /tmp/spark.tgz \
    && chown -R airflow:root /opt/spark

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt
