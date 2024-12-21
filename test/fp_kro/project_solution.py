from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default arguments
default_args = {
    "owner": "airflow",  
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

# Function to create SparkSubmitOperator tasks
def create_spark_task(task_id, script_path, job_name):
    return SparkSubmitOperator(
        task_id=task_id,
        application=script_path,
        conn_id="spark-default",
        name=job_name,
        verbose=True,
        conf={
            "spark.executor.memory": "4g",
            "spark.driver.memory": "4g",
            "spark.executor.cores": "4",
        },
    )

# DAG definition
with DAG(
    "batch_pipeline_dag",  
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    
    landing_to_bronze = create_spark_task(
        task_id="landing_to_bronze",  
        script_path="/opt/airflow/dags/landing_to_bronze.py",
        job_name="landing_to_bronze_job",  
    )

    bronze_to_silver = create_spark_task(
        task_id="bronze_to_silver",  
        script_path="/opt/airflow/dags/bronze_to_silver.py",
        job_name="bronze_to_silver_job",  
    )

    silver_to_gold = create_spark_task(
        task_id="silver_to_gold",  
        script_path="/opt/airflow/dags/silver_to_gold.py",
        job_name="silver_to_gold_job",  
    )

    # Task dependencies
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
