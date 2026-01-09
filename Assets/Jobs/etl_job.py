"""
StreamFlow ETL Job - PySpark Transformation Pipeline

Reads JSON from landing zone, applies transformations, writes CSV to gold zone.

Pattern: ./data/landing/*.json -> (This Job) -> ./data/gold/
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_session_factory import create_spark_session


"""
We would have two input paths created,
One for user events and another for transaction events
We would want to join the two tables together
Do some cleaning
and send it back to the gold zone
"""
def run_etl(spark: SparkSession, input_path_topic1: str, input_path_topic2: str, output_path: str):
    """
    Main ETL pipeline: read -> transform -> write.
    
    Args:
        spark: Active SparkSession
        input_path: Landing zone path (e.g., '/opt/spark-data/landing/*.json')
        output_path: Gold zone path (e.g., '/opt/spark-data/gold')
    """
    # TODO: Implement
    spark = create_spark_session("ETL_job")
    
    df_topic1 = spark.read.json(input_path_topic1)
    df_topic2 = spark.read.json(input_path_topic2)
    
    
    pass


if __name__ == "__main__":
    # TODO: Create SparkSession, parse args, run ETL
    pass
