"""
StreamFlow Analytics Platform - Main Orchestration DAG

Orchestrates: Kafka Ingest -> Spark ETL -> Validation
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'student',
    'topics': ['user_events','transaction_events']
    # TODO: Add retry logic, email alerts, etc.
}

with DAG(
    dag_id='streamflow_main',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    # Creates the topic for the kafka server if it doesn't exist
    create_topics = BashOperator(
        task_id = "create_kafka_topics",
        bash_command = 'echo "kafka-topics --boostrap-server kafka:9092 \
                --create --if-not-exist \
                --topic user_events"; echo"\
            kafka-topics --boostrap-server kafka:9092 \
                --create --if-not-exist \
                --topic transaction_events"',
        dag = dag
    )
    
    
    
    # TODO: Define tasks
    # - ingest_kafka: Run ingest_kafka_to_landing.py
    # - spark_etl: spark-submit etl_job.py
    # - validate: Check output files
    
    # TODO: Set dependencies
    pass
