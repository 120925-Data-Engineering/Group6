"""
StreamFlow Analytics Platform - Main Orchestration DAG

Orchestrates: Kafka Ingest -> Spark ETL -> Validation
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
from pathlib import Path
import os

SPARK_JOBS_PATH = '/opt/spark-jobs'
# TIME_DURATION = '30' # In seconds 
FIRST_TOPIC = 'user_events'
SECOND_TOPIC = 'transaction_events'
BRONZE_PATH = '/opt/spark-data/landing'
GOLD_PATH = '/opt/spark-data/gold'


default_args = {
    'owner': 'student',
    'retries' : 0,
    'trigger_rule': 'all_success'
    # TODO: Add retry logic, email alerts, etc.
}

# UNIMPLEMENTED!
# We are trying to get the kafka information from connection
# def get_kafka_details(**context):
#     """
#     We are trying to collect the kafka information that is stored in the airflow connections using basehook
    
#     """
#     print("Collecting connection details")
    
#     try:
#         kafka_connection = BaseHook.get_connection("kafka_connection")
        
#         # Our kafka information is stored in the extra box
#         if kafka_connection.extra:
#             extra = kafka_connection.extra_dejson
#             print("Kafka Configurations")
            
#             # Makes the configurations into a xcomm variable
#             return extra
        
#         else:
#             print("we found no kafka configurations")
#             return {}
    
#     except Exception as e:
#         print(f"Connection 'kafka_connection' is not found: {e}")
#         print("Please create the connection in the airflow UI")
#         return {}

def upload_local_files_to_snowflake_transaction_csv(gold_layer: str,table_name: str, **context):
    hook = SnowflakeHook(snowflake_conn_id = "snowflake_connection")
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    local_gold_dir = f'/opt/spark-data/gold/'
    
    spark_files = [f for f in Path(local_gold_dir, gold_layer).rglob("*.csv")]

    # print(spark_files)
    # print(Path(local_gold_dir, gold_layer))
    print("Zach")
    print(context)
    
    cursor.execute("USE WAREHOUSE COMPUTE_WH")
    
    for file_path in spark_files:
        
        print(f"Uploading {file_path} to snowflake stage @%{table_name}")
        cursor.execute(f"PUT file://{file_path} @BRONZE.CSV_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    if table_name == "transactions_all":
        cursor.execute(f"""
                        COPY INTO {table_name} (raw_data, source_file)
                        FROM (
                            SELECT
                                OBJECT_CONSTRUCT(
                                'currency', $1,
                                'original_transaction_id', $2,
                                'payment_method', $3,
                                'status', $4,
                                'subtotal', $5,
                                'tax', $6,
                                'timestamp', $7,
                                'total', $8,
                                'transaction_id', $9,
                                'transaction_type', $10,
                                'user_id', $11,
                                'shipping_address_city', $12,
                                'shipping_address_country', $13,
                                'shipping_address_state', $14,
                                'shipping_address_street', $15,
                                'shipping_address_zip_code', $16,
                                'billing_address_city', $17,
                                'billing_address_country', $18,
                                'billing_address_state', $19,
                                'billing_address_street', $20,
                                'billing_address_zip_code', $21,
                                'brand', $22,
                                'category', $23,
                                'product_name', $24,
                                'product_id', $25,
                                'quantity', $26,
                                'unit_price', $27
                                    ) as raw_data,
                                METADATA$FILENAME as source_file
                            FROM @BRONZE.CSV_STAGE
                            )
                        FILE_FORMAT = (FORMAT_NAME = 'BRONZE.csv_format')
                        ON_ERROR = 'CONTINUE';

                        """)
    else:
        cursor.execute(f"""
                        COPY INTO {table_name} (raw_data, source_file)
                        FROM (
                            SELECT
                                OBJECT_CONSTRUCT(
                                'browser', $1,
                                'city', $2,
                                'country', $3,
                                'device', $4,
                                'element_id', $5,
                                'event_id', $6,
                                'event_type', $7,
                                'ip_address', $8,
                                'page', $9,
                                'product_id', $10,
                                'quantity', $11,
                                'search_query', $12,
                                'session_id', $13,
                                'timestamp', $14,
                                'user_id', $15
                                    ) as raw_data,
                                METADATA$FILENAME as source_file
                            FROM @BRONZE.CSV_STAGE
                            )
                        FILE_FORMAT = (FORMAT_NAME = 'BRONZE.csv_format')
                        ON_ERROR = 'CONTINUE';

                        """)
    
    cursor.execute("REMOVE @BRONZE.CSV_STAGE")
    conn.commit()
    cursor.close()
    conn.close()



with DAG(
    dag_id='streamflow_main',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    # Notify dag start
    start = EmptyOperator(task_id = 'start')
    
    end = EmptyOperator(task_id = 'end')
    
    # First thing we need to do is collect from the topics
    # by the consumers we have
    # kafka_consumers_user = BashOperator(
    #     task_id = "kafka_consumers_users",
    #     bash_command = f"""
    #         echo "Starting Kafka consumer"
    #         python {SPARK_JOBS_PATH}/ingest_kafka_to_landing.py \\
    #             --topic {FIRST_TOPIC} \\
    #             --batch-time {TIME_DURATION} \\
    #             --output-path {BRONZE_PATH}
    #     """
    # )
    
    # kafka_consumers_transaction = BashOperator(
    #     task_id = "kafka_consumers_transaction",
    #     bash_command = f"""
    #         echo "Starting Kafka consumer"
    #         python {SPARK_JOBS_PATH}/ingest_kafka_to_landing.py \\
    #             --topic {SECOND_TOPIC} \\
    #             --batch-time {TIME_DURATION} \\
    #             --output-path {BRONZE_PATH}
    #     """
    # )
    
    
    upload_to_stage_trans = PythonOperator(
        task_id = "Snowflake_loading_files_transaction_all",
        python_callable=upload_local_files_to_snowflake_transaction_csv,
        op_kwargs={
        "gold_layer" : "transactions_all",
        "table_name": "BRONZE.raw_transaction_events"
        }
    )
    
    upload_to_stage_user = PythonOperator(
        task_id = "Snowflake_loading_files_user_events_all",
        python_callable=upload_local_files_to_snowflake_transaction_csv,
        op_kwargs={
        "gold_layer" : "user_events_all",
        "table_name": "BRONZE.raw_user_events"
        }
    )
    
    submit_spark_job = BashOperator(
        task_id = "Spark_Submit",
        bash_command = """
            spark-submit \
                --master spark://spark-master:7077 \
                /opt/spark-jobs/etl_job.py
        """
    )
    # [kafka_consumers_transaction, kafka_consumers_user]
    start >> submit_spark_job >> upload_to_stage_trans >> upload_to_stage_user >> end
    
    
    
    # TODO: Define tasks
    # - ingest_kafka: Run ingest_kafka_to_landing.py
    # - spark_etl: spark-submit etl_job.py
    # - validate: Check output files
    
    # TODO: Set dependencies
    
