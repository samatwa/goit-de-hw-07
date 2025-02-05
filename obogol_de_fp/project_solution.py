from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

# Базовий шлях до скриптів
BASE_PATH = os.getenv('BASE_PATH', '/opt/airflow/dags')

# Параметри DAG за замовчуванням
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # DAG стартує з учорашнього дня
    'retries': 1,  # Повторити завдання у випадку невдачі
}

# Створення DAG
dag = DAG(
    'multi_hop_datalake',  # Ім'я DAG
    default_args=default_args,
    description='ETL pipeline from landing to gold zone',
    schedule_interval=None,  # Запуск тільки вручну
    catchup=False,
    tags=['datalake', 'etl', 'multi-hop'],
)

# Завдання 1: Запуск landing_to_bronze.py
landing_to_bronze = BashOperator(
    task_id='landing_to_bronze',
    bash_command=f'python {BASE_PATH}/landing_to_bronze.py',
    dag=dag,
)

# Завдання 2: Запуск bronze_to_silver.py
bronze_to_silver = BashOperator(
    task_id='bronze_to_silver',
    bash_command=f'python {BASE_PATH}/bronze_to_silver.py',
    dag=dag,
)

# Завдання 3: Запуск silver_to_gold.py
silver_to_gold = BashOperator(
    task_id='silver_to_gold',
    bash_command=f'python {BASE_PATH}/silver_to_gold.py',
    dag=dag,
)

# Послідовність виконання завдань

landing_to_bronze >> bronze_to_silver >> silver_to_gold
