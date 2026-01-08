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


default_args = {
    'owner': 'student',
    # TODO: Add retry logic, email alerts, etc.
}

# We are trying to get the kafka information from connection
def get_kafka_details(**context):
    """
    We are trying to collect the kafka information that is stored in the airflow connections using basehook
    
    """
    print("Collecting connection details....Please wait")
    
    try:
        kafka_connection = BaseHook.get_connection("kafka_connection")
        
        # Our kafka information is stored in the extra box
        if kafka_connection.extra:
            extra = kafka_connection.extra_dejson
            print("Kafka Configurations")
            
            for key, value in extra.items():
                print(f"{key}   :   {value}")
            
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
    
    # Collecting the kafka information from connections
    get_config_details = PythonOperator(
        task_id = "get_kafka_config",
        python_callable = get_kafka_details
    )
    
    # Creates the topic for the kafka server if it doesn't exist
    create_topics = BashOperator(
        task_id = "create_kafka_topics",
        bash_command = 'echo "kafka-topics --boostrap-server kafka:9092 \
                --create --if-not-exists \
                --topic user_events"; echo"\
            kafka-topics --boostrap-server kafka:9092 \
                --create --if-not-exists \
                --topic transaction_events"',
        dag = dag
    )
    
    running_producers = BashOperator(
        task_id = "Connecting_the_producers",
        bash_command = ''
    )
    
    
    
    
    
    # TODO: Define tasks
    # - ingest_kafka: Run ingest_kafka_to_landing.py
    # - spark_etl: spark-submit etl_job.py
    # - validate: Check output files
    
    # TODO: Set dependencies
    pass
