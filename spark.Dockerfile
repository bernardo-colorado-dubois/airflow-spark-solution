# syntax=docker/dockerfile:1
#
# Custom image for spark-master/spark-worker, built on top of the official
# apache/spark image (SPARK_IMAGE_NAME in .env). The stock image ships no
# Python libs beyond PySpark itself (see CLAUDE.md, "Decisiones no obvias",
# punto 10) -- fine for pure DataFrame/SQL jobs, but jobs that use
# applyInPandas/pandas UDFs run that code on the executors, which need
# pandas/pyarrow installed there, not just on the Airflow driver.
#
# Versions pinned to match what requirements.txt (pandas>=2.2) resolves on
# the driver image, so Arrow-serialized data round-trips identically between
# driver and executors.
ARG SPARK_BASE_IMAGE
FROM ${SPARK_BASE_IMAGE}

USER root
RUN pip3 install --no-cache-dir pandas==2.3.3 pyarrow==25.0.0 numpy==2.2.6
