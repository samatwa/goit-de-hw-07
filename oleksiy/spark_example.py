from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Define default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'spark_example',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['oleksiy']
)

# Define the SparkSubmitOperator
spark_submit_task = SparkSubmitOperator(
    application='dags/oleksiy/spark_job.py',  # Path to your Spark job
    task_id='spark_submit_job',
    conn_id='spark-default',  # Spark connection ID
    verbose=1,
    dag=dag,
)

# Set task dependencies (if any)
spark_submit_task
