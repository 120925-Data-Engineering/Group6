"""
SparkSession Factory Module

Provides factory functions for creating SparkSession instances.
"""
from pyspark.sql import SparkSession
from typing import Optional


def create_spark_session(
    app_name: str,
    master: str = "local[*]",
    config_overrides: Optional[dict] = None

):
    """
    Create and return a configured SparkSession.

    Args:
        app_name (str): Name for the Spark application
        master (str): Spark master URL (default: local[*])
        config_overrides (dict, optional): Extra Spark configs

    Returns:
        SparkSession: Configured SparkSession instance
    """

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
    )


    # can apply optional sspark configuration overrides
    if config_overrides:
        for key, value in config_overrides.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    return spark


# optional - allows file to be run directly for fast testing
if __name__ == "__main__":
    spark = create_spark_session("LocalSparkTest")

    print("Spark master:", spark.sparkContext.master)
    print("Spark version:", spark.version)

    spark.stop()
