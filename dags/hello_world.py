"""DAG mínimo: envía spark/hello_world.py al cluster de Spark standalone."""

from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

with DAG(
    dag_id="hello_world",
    description="Corre el job PySpark hello_world en el cluster de Spark",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "example"],
):
    SparkSubmitOperator(
        task_id="hello_world",
        conn_id="spark_default",
        application="/opt/spark-apps/hello_world.py",
        name="hello_world",
    )
