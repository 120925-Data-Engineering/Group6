"""
StreamFlow ETL Job - PySpark Transformation Pipeline

Reads JSON from landing zone, applies transformations, writes CSV to gold zone.

Pattern: ./data/landing/*.json -> (This Job) -> ./data/gold/
"""
from spark_session_factory import create_spark_session
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType


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
    
    
    return df_topic_transaction, df_topic_user

def advertising_gold_zone(spark: SparkSession, df_topic_transaction, df_topic_user, output_path: str):
    
    df_exploded_transaction = df_topic_transaction.withColumn("product", F.explode("products")) \
        .withColumnRenamed("timestamp", "t_timestamp")
        
    df_topics_combined = df_exploded_transaction.alias("transaction") \
        .join(df_topic_user.alias("user"), 
              ((F.col("user.user_id") == F.col("transaction.user_id")) & (F.col("user.product_id") == F.col("transaction.product.product_id"))), 
              "inner")
    
    df_advertising_dept = df_topics_combined \
        .select("products","browser",
            "transaction.user_id","t_timestamp", "device", 
            "shipping_address.state", "shipping_address.city", "shipping_address.country") \
                .withColumn("product", F.explode("products")).drop("products")
                
    prod_column_names = df_advertising_dept.select("product").schema["product"].dataType.__dict__["names"]
    
    df_advertising_dept = df_advertising_dept.select("*",*[F.col(f"product.{x}").alias(x) for x in prod_column_names]).drop("product") \
        .withColumn("line_revenue", F.round(F.col("quantity")*F.col("unit_price"),2).cast(DecimalType(10,2)))
    
    df_advertising_dept.coalesce(1)

    df_advertising_dept.write.csv(f"{output_path}/advertising",mode = "overwrite", header = True)
    

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
    
    df_topic_transaction, df_topic_user = reading_data_from_landing(spark, input_path_topic1, input_path_topic2)
    
    advertising_gold_zone(spark,df_topic_transaction,df_topic_user,output_path)
    
    
    pass


if __name__ == "__main__":
    # TODO: Create SparkSession, parse args, run ETL
    
    run_etl(create_spark_session("ETL_job"),'./Assets/data/landing/transaction_events_1767974907.7832654.json', './Assets/data/landing/user_events_1767974827.1332347.json', "./Assets/data/gold")
    pass
