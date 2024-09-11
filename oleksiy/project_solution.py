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
    'project_solution',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['oleksiy']
)

# Define the SparkSubmitOperator
landing_to_bronze = SparkSubmitOperator(
    application='dags/oleksiy/landing_to_bronze.py',  # Path to your Spark job
    task_id='landing_to_bronze',
    conn_id='spark-default',  # Spark connection ID
    verbose=1,
    dag=dag,
)

bronze_to_silver = SparkSubmitOperator(
    application='dags/oleksiy/bronze_to_silver.py',  # Path to your Spark job
    task_id='bronze_to_silver',
    conn_id='spark-default',  # Spark connection ID
    verbose=1,
    dag=dag,
)

silver_to_gold = SparkSubmitOperator(
    application='dags/oleksiy/silver_to_gold.py',  # Path to your Spark job
    task_id='silver_to_gold',
    conn_id='spark-default',  # Spark connection ID
    verbose=1,
    dag=dag,
)

# Set task dependencies (if any)
landing_to_bronze >> bronze_to_silver >> silver_to_gold
