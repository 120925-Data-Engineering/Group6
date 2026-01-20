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

SPARK_JOBS_PATH = '/opt/spark-jobs'
# TIME_DURATION = '30' # In seconds 
FIRST_TOPIC = 'user_events'
SECOND_TOPIC = 'transaction_events'
BRONZE_PATH = '/opt/spark-data/landing'
GOLD_PATH = '/opt/spark-data/gold'


default_args = {
    'owner': 'student',
    'retries' : 1,
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

def upload_local_files_to_snowflake(folder: str, table_name: str, **context):
    hook = SnowflakeHook(snowflake_conn_id = "snowflake_connection")
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    local_gold_dir = f'/opt/spark_data/gold/{folder}'
    
    spark_files = [f for f in local_gold_dir.rglob("*.csv")]
    
    for file_path in spark_files:
        print(f"Uploading {file_path} to snowflake stage @%{table_name}")
        cursor.execute(f"PUT file://{file_path} @BRONZE.CSV_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    
    cursor.execute(f"""
            COPY INTO {table_name}(raw_data, source_file)
            FROM (
                SELECT
                    $1 AS raw_data,
                    METADATA$FILENAME AS source_file,
                FROM @BRONZE.CSV_STAGE
            )
            FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE'
        """)
    
    cursor.execute("REMOVE @CSV_STAGE")
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
    
    
    upload_to_stage = PythonOperator(
        task_id = "Snowflake_loading_files",
        python_callable=upload_local_files_to_snowflake,
        op_kwargs={
        "folder": "transactions_all",
        "table_name": "BRONZE.user_events"
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
    start >> submit_spark_job >> upload_to_stage >> end
    
    
    
    # TODO: Define tasks
    # - ingest_kafka: Run ingest_kafka_to_landing.py
    # - spark_etl: spark-submit etl_job.py
    # - validate: Check output files
    
    # TODO: Set dependencies
    
