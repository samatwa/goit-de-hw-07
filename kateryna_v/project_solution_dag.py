from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Параметри за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

# Створення DAG
dag = DAG(
    'data_lake_dag_kv',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["kateryna_v"]
)

# Завдання для кожного етапу
landing_to_bronze_task = SparkSubmitOperator(
    application='dags/kateryna_v/landing_to_bronze.py',
    task_id='landing_to_bronze',
    conn_id='spark_default',
    verbose=1,
    dag=dag,
)

bronze_to_silver_task = SparkSubmitOperator(
    application='dags/kateryna_v/bronze_to_silver.py',
    task_id='bronze_to_silver',
    conn_id='spark_default',
    verbose=1,
    dag=dag,
)

silver_to_gold_task = SparkSubmitOperator(
    application='dags/kateryna_v/silver_to_gold.py',
    task_id='silver_to_gold',
    conn_id='spark_default',
    verbose=1,
    dag=dag,
)

# Визначення послідовності виконання завдань
landing_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task