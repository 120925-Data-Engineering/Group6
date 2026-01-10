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
from pathlib import Path

"""
We would have two input paths created,
One for user events and another for transaction events
We would want to join the two tables together
Do some cleaning
and send it back to the gold zone
"""
def collecting_bronze_layer(starting_path: str = "./Assests/data/landing"):
    # Collects the two file names
    transaction_file = next(Path(starting_path).glob("transaction*.json"),None)
    user_file = next(Path(starting_path).glob("user*.json"), None)
    # Returns the two files we expect
    # print(transaction_file, user_file)
    return transaction_file, user_file
    

def reading_data_from_landing(spark: SparkSession, input_path_topic1: str, input_path_topic2: str):
    # Reads the data into spark
    df_topic_transaction = spark.read.json(f"./{input_path_topic1}")
    df_topic_user = spark.read.json(f"./{input_path_topic2}")
    
    # returns the spark dataframes
    return df_topic_transaction, df_topic_user

def advertising_gold_zone(spark: SparkSession, df_topic_transaction, df_topic_user, output_path: str):
    
    # Expands the products into their own columns
    df_exploded_transaction = df_topic_transaction.withColumn("product", F.explode("products")) \
        .withColumnRenamed("timestamp", "t_timestamp")
    
    # Combines the user and transaction tables on user id and product id
    # can lead to a cartesian effect on tables but the best I can do with the tables provided
    df_topics_combined = df_exploded_transaction.alias("transaction") \
        .join(df_topic_user.alias("user"), 
              ((F.col("user.user_id") == F.col("transaction.user_id")) & (F.col("user.product_id") == F.col("transaction.product.product_id"))), 
              "inner")

    # Selects the needed rows for the advertising department
    # and expands products
    df_advertising_dept = df_topics_combined \
        .select("products","browser",
            "transaction.user_id","t_timestamp", "device", 
            "shipping_address.state", "shipping_address.city", "shipping_address.country") \
                .withColumn("product", F.explode("products")).drop("products")
            
    # Collects the names of each product column
    prod_column_names = df_advertising_dept.select("product").schema["product"].dataType.__dict__["names"]
    
    # Creates a new colmn for each product like product_id,name,unit price and quantity
    df_advertising_dept = df_advertising_dept.select("*",*[F.col(f"product.{x}").alias(x) for x in prod_column_names]).drop("product") \
        .withColumn("line_revenue", F.round(F.col("quantity")*F.col("unit_price"),2).cast(DecimalType(10,2)))
    
    # combines the table to one file
    df_advertising_dept.coalesce(1)

    # saves it to the gold layer
    df_advertising_dept.write.csv(f"{output_path}/advertising",mode = "overwrite", header = True)
    

def run_etl(spark: SparkSession, input_path: str, output_path: str):
    """
    Main ETL pipeline: read -> transform -> write.
    
    Args:
        spark: Active SparkSession
        input_path: Landing zone path (e.g., '/opt/spark-data/landing/*.json')
        output_path: Gold zone path (e.g., '/opt/spark-data/gold')
    """
    # TODO: Implement
    # Collects the spark session
    spark = spark
    
    # Collects the path of the files in bronze layer
    transaction_path, events_path = collecting_bronze_layer(input_path)
    
    # Collects the dataframes after reading the json files
    df_topic_transaction, df_topic_user = reading_data_from_landing(spark, transaction_path, events_path)
    
    # Creates the gold zone
    advertising_gold_zone(spark,df_topic_transaction,df_topic_user,output_path)
    
    spark.stop()


if __name__ == "__main__":
    # TODO: Create SparkSession, parse args, run ETL
    
    run_etl(create_spark_session("ETL_job"),'./Assets/data/landing', "./Assets/data/gold")
