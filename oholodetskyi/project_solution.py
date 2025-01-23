from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    # Дата у майбутньому, щоб Airflow не стартував "у минулому"
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="project_solution_oholodetskyi",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["oholodetskyi"]
) as dag:

    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application=(
            "C:/Users/Sanr/Desktop/Data-Engineering/PycharmProjects/"
            "PythonProject/goit-de-fp/Building an End-to-End Batch Data Lake/"
            "landing_to_bronze.py"
        ),
        conn_id="spark_default",
        verbose=True,
        name="landing_to_bronze_oholodetskyi",
        execution_timeout=None,
        # ГОЛОВНЕ: вказати локальний режим Spark:
        conf={
            "spark.master": "local[*]"
        }
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=(
            "C:/Users/Sanr/Desktop/Data-Engineering/PycharmProjects/"
            "PythonProject/goit-de-fp/Building an End-to-End Batch Data Lake/"
            "bronze_to_silver.py"
        ),
        conn_id="spark_default",
        verbose=True,
        name="bronze_to_silver_oholodetskyi",
        execution_timeout=None,
        conf={
            "spark.master": "local[*]"
        }
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application=(
            "C:/Users/Sanr/Desktop/Data-Engineering/PycharmProjects/"
            "PythonProject/goit-de-fp/Building an End-to-End Batch Data Lake/"
            "silver_to_gold.py"
        ),
        conn_id="spark_default",
        verbose=True,
        name="silver_to_gold_oholodetskyi",
        execution_timeout=None,
        conf={
            "spark.master": "local[*]"
        }
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
