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

def reading_data_from_landing(spark: SparkSession, input_path_topic1: str, input_path_topic2: str):
    
    df_topic_transaction = spark.read.json(input_path_topic1)
    df_topic_user = spark.read.json(input_path_topic2)
    
    
    return (df_topic_transaction, df_topic_user)


def run_etl(spark: SparkSession, input_path_topic1: str, input_path_topic2: str, output_path: str):
    """
    Main ETL pipeline: read -> transform -> write.
    
    Args:
        spark: Active SparkSession
        input_path: Landing zone path (e.g., '/opt/spark-data/landing/*.json')
        output_path: Gold zone path (e.g., '/opt/spark-data/gold')
    """
    # TODO: Implement
    spark = spark
    
    df_topic1 = spark.read.json(input_path_topic1)
    df_topic2 = spark.read.json(input_path_topic2)
    
    df_topic1.show(1)
    
    
    pass


if __name__ == "__main__":
    # TODO: Create SparkSession, parse args, run ETL
    
    run_etl(create_spark_session("ETL_job"),'./Assets/data/landing/transaction_events_1767974907.7832654.json', './Assets/data/landing/user_events_1767974827.1332347.json', "")
    pass
