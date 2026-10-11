"""Job PySpark mínimo: arma un DataFrame chico y lo procesa en los executors."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    spark = SparkSession.builder.appName("hello_world").getOrCreate()

    df = spark.createDataFrame(
        [("hello", 1), ("world", 2), ("hello", 3), ("spark", 4)],
        ["word", "value"],
    )

    result = (
        df.groupBy("word")
        .agg(F.count("*").alias("count"), F.sum("value").alias("total"))
        .orderBy("word")
    )
    result.show()

    print(f"Hello World desde Spark {spark.version} — {df.count()} filas procesadas")

    spark.stop()


if __name__ == "__main__":
    main()
