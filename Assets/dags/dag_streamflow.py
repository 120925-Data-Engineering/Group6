"""
StreamFlow Analytics Platform - Main Orchestration DAG

Orchestrates: Kafka Ingest -> Spark ETL -> Validation
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

SPARK_JOBS_PATH = '/opt/spark-jobs'
TIME_DURATION = '180' # In seconds
FIRST_TOPIC = 'user_events'
SECOND_TOPIC = 'transaction_events'
BRONZE_PATH = '/opt/spark-data/landing'
GOLD_PATH = '/opt/spark-data/gold'


default_args = {
    'owner': 'student',
    # TODO: Add retry logic, email alerts, etc.
}

# We are trying to get the kafka information from connection
def get_kafka_details(**context):
    """
    We are trying to collect the kafka information that is stored in the airflow connections using basehook
    
    """
    print("Collecting connection details")
    
    try:
        kafka_connection = BaseHook.get_connection("kafka_connection")
        
        # Our kafka information is stored in the extra box
        if kafka_connection.extra:
            extra = kafka_connection.extra_dejson
            print("Kafka Configurations")
            
            # Makes the configurations into a xcomm variable
            return extra
        
        else:
            print("we found no kafka configurations")
            return {}
    
    except Exception as e:
        print(f"Connection 'kafka_connection' is not found: {e}")
        print("Please create the connection in the airflow UI")
        return {}
    


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
    kafka_consumers_user = BashOperator(
        task_id = "kafka_consumers",
        bash_command = f"""
            echo "Starting Kafka consumer"
            python {SPARK_JOBS_PATH}/ingest_kafka_to_landing.py \\
                --topic {FIRST_TOPIC} \\
                --batch-time {TIME_DURATION} \\
                --output-path {BRONZE_PATH}
        """
    )
    
    kafka_consumers_transaction = BashOperator(
        task_id = "kafka_consumers_transaction",
        bash_command = f"""
            echo "Starting Kafka consumer"
            python {SPARK_JOBS_PATH}/ingest_kafka_to_landing.py \\
                --topic {SECOND_TOPIC} \\
                --batch-time {TIME_DURATION} \\
                --output-path {BRONZE_PATH}
        """
    )
    
    # submit_spark_job = BashOperator(
    #     task_id = "Spark_Submit",
    #     bash_command = """
    #         spark-submit \
    #             --master 
    #     """
    # )
    
    start >> [kafka_consumers_transaction, kafka_consumers_user] >> end
    
    
    
    # TODO: Define tasks
    # - ingest_kafka: Run ingest_kafka_to_landing.py
    # - spark_etl: spark-submit etl_job.py
    # - validate: Check output files
    
    # TODO: Set dependencies
    
