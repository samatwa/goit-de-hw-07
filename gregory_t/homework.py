from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.sql import SqlSensor
import random
import time

# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Функція для вибору випадкового значення медалі
def choose_medal_type(**kwargs):
    medal = random.choice(['Bronze', 'Silver', 'Gold'])
    kwargs['ti'].xcom_push(key='medal_type', value=medal)
    return medal

# Функція для затримки
def delay_task():
    time.sleep(25)  # 35 секунд затримки

# Визначення DAG
with DAG(
    'gregory_medal_count_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['gregory']
) as dag:

    # Завдання 1: Створення таблиці
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='goit_mysql_db_gregory',
        sql="""
        CREATE TABLE IF NOT EXISTS olympic_dataset.gregory_medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Завдання 2: Вибір типу медалі
    choose_medal = PythonOperator(
        task_id='choose_medal',
        python_callable=choose_medal_type,
        provide_context=True
    )

    # Dummy оператори для розгалуження
    branch_bronze = DummyOperator(task_id='branch_bronze')
    branch_silver = DummyOperator(task_id='branch_silver')
    branch_gold = DummyOperator(task_id='branch_gold')

    # Завдання 3: Розгалуження
    def branch_logic(**kwargs):
        medal_type = kwargs['ti'].xcom_pull(task_ids='choose_medal', key='medal_type')
        return f"branch_{medal_type.lower()}"

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_logic,
        provide_context=True
    )

    # Завдання 4: Логіка для Bronze
    count_bronze = MySqlOperator(
        task_id='count_bronze',
        mysql_conn_id='goit_mysql_db_gregory',
        sql="""
        INSERT INTO olympic_dataset.gregory_medal_counts (medal_type, count)
        SELECT 'Bronze', COUNT(*) FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """
    )

    # Завдання 4: Логіка для Silver
    count_silver = MySqlOperator(
        task_id='count_silver',
        mysql_conn_id='goit_mysql_db_gregory',
        sql="""
        INSERT INTO olympic_dataset.gregory_medal_counts (medal_type, count)
        SELECT 'Silver', COUNT(*) FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """
    )

    # Завдання 4: Логіка для Gold
    count_gold = MySqlOperator(
        task_id='count_gold',
        mysql_conn_id='goit_mysql_db_gregory',
        sql="""
        INSERT INTO olympic_dataset.gregory_medal_counts (medal_type, count)
        SELECT 'Gold', COUNT(*) FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """
    )

    # Завдання 5: Затримка
    delay = PythonOperator(
        task_id='delay_task',
        python_callable=delay_task
    )

    # Завдання 6: Перевірка останнього запису
    check_recent_record = SqlSensor(
        task_id='check_recent_record',
        conn_id='goit_mysql_db_gregory',
        sql="""
        SELECT CASE
            WHEN TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30 THEN 1
            ELSE 0
        END
        FROM olympic_dataset.gregory_medal_counts;
        """,
        mode='poke',
        poke_interval=5,
        timeout=60
    )

    # Встановлення залежностей
    create_table >> choose_medal >> branch_task

    branch_task >> branch_bronze >> count_bronze >> delay >> check_recent_record
    branch_task >> branch_silver >> count_silver >> delay >> check_recent_record
    branch_task >> branch_gold >> count_gold >> delay >> check_recent_record
