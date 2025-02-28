from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

# DAG для послідовного запуску ETL-процесу batch Data Lake
with DAG(
        'nvi_fin_project_datalake_solution',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["nvi_datalake"]
) as dag:
    landing_to_bronze = SparkSubmitOperator(
        task_id='landing_to_bronze',
        application='dags/nvi_fp_dags_v6/nvi_spark_jobs/2_01_landing_to_bronze.py',  # Змініть шлях за потребою
        conn_id='spark-default',
        verbose=1,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application='dags/nvi_fp_dags_v6/nvi_spark_jobs/2_02_bronze_to_silver.py',  # Змініть шлях за потребою
        conn_id='spark-default',
        verbose=1,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application='dags/nvi_fp_dags_v6/nvi_spark_jobs/2_03_silver_to_gold.py',  # Змініть шлях за потребою
        conn_id='spark-default',
        verbose=1,
    )

    # Послідовне виконання завдань
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
