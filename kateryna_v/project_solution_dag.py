from airflow import DAG
from airflow.operators.bash import BashOperator
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
landing_to_bronze_task = BashOperator(
    task_id='landing_to_bronze',
    bash_command='python /path/to/your/dags/landing_to_bronze.py',
    dag=dag,
)

bronze_to_silver_task = BashOperator(
    task_id='bronze_to_silver',
    bash_command='python /path/to/your/dags/bronze_to_silver.py',
    dag=dag,
)

silver_to_gold_task = BashOperator(
    task_id='silver_to_gold',
    bash_command='python /path/to/your/dags/silver_to_gold.py',
    dag=dag,
)

# Визначення послідовності виконання завдань
landing_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task