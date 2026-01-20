from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_connection')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT current_version()")
    version = cursor.fetchone()[0]
    print(f"✅ Connected to Snowflake! Version: {version}")
    cursor.close()
    conn.close()

with DAG(
    dag_id='test_snowflake_connection',
    start_date=datetime(2026, 1, 20),
    schedule_interval=None,
    catchup=False,
    tags=['test'],
) as dag:

    test_connection = PythonOperator(
        task_id='test_connection',
        python_callable=test_snowflake_connection
    )