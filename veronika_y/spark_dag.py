from airflow import DAG
from datetime import datetime
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

# Визначення DAG
with DAG(
        'spark_db_v',
        default_args=default_args,
        schedule_interval=None,  # DAG не має запланованого інтервалу виконання
        catchup=False,  # Вимкнути запуск пропущених задач
        tags=["veronika_y"]  # Теги для класифікації DAG
) as dag:
    spark_submit_task = SparkSubmitOperator(
        application='veronika_y/spark_test.py',
        task_id='spark_submit_job',
        conn_id='spark-default',
        verbose=1,
        dag=dag)


    spark_submit_task
