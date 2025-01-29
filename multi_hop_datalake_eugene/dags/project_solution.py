from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

# Налаштування за замовчуванням
default_args = {
    'owner': 'eugene',
    'start_date': datetime(2024, 8, 4),
}

# 🔍 Шлях до скриптів
SCRIPTS_DIR = "/root/airflow-docker/dags/scripts"

# Створення DAG
with DAG(
    dag_id='multi_hop_datalake_eugene',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["eugene"],
    description='Pipeline for multi-hop datalake: landing → bronze → silver → gold',
) as dag:

    # Landing to bronze
    landing_to_bronze_all = SparkSubmitOperator(
        application=f"{SCRIPTS_DIR}/landing_to_bronze.py",
        task_id='landing_to_bronze_all_tables',
        conn_id='spark-default',
        verbose=1,
        dag=dag
    )

    # Bronze to silver
    bronze_to_silver_all = SparkSubmitOperator(
        application=f"{SCRIPTS_DIR}/bronze_to_silver.py",
        task_id='bronze_to_silver_all_tables',
        conn_id='spark-default',
        verbose=1,
        dag=dag
    )

    # Silver to gold (агрегація)
    silver_to_gold_avg_stats = SparkSubmitOperator(
        application=f"{SCRIPTS_DIR}/silver_to_gold.py",
        task_id='silver_to_gold_avg_stats',
        conn_id='spark-default',
        verbose=1,
        dag=dag
    )

    # Послідовність виконання
    landing_to_bronze_all >> bronze_to_silver_all >> silver_to_gold_avg_stats
