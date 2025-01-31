from datetime import datetime
import random
import time
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

# Конфігурація DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0
}

# Параметри підключення (використовуємо правильний conn_id)
MYSQL_CONN_ID = 'goit_mysql_db_dag_palchyk1984'
DATABASE_NAME = 'dag_palchyk1984'
TABLE_NAME = f'{DATABASE_NAME}.medals_log'

with DAG(
    dag_id='medals_dag_palchyk1984',
    description='DAG for medals counting with random branching',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dag_palchyk1984']
) as dag:

    # 1. Створення бази даних
    create_database = MySqlOperator(
        task_id='create_database',
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME};"
    )

    # 2. Створення таблиці в базі
    create_medals_table = MySqlOperator(
        task_id='create_medals_table',
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # 3. Випадковий вибір медалі
    def _choose_medal(**context):
        chosen = random.choice(['Bronze', 'Silver', 'Gold'])
        context['ti'].xcom_push(key='picked_medal', value=chosen)

    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=_choose_medal
    )

    # 4. Розгалуження залежно від вибору медалі
    def _branch_medal(**context):
        return f"count_{context['ti'].xcom_pull(key='picked_medal', task_ids='pick_medal').lower()}"

    branch = BranchPythonOperator(
        task_id='branch_on_medal',
        python_callable=_branch_medal
    )

    # 5. Завдання підрахунку медалей
    count_bronze = MySqlOperator(
        task_id='count_bronze',
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (medal_type, count)
        SELECT 'Bronze', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Bronze' LOCK IN SHARE MODE;
        """
    )

    count_silver = MySqlOperator(
        task_id='count_silver',
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (medal_type, count)
        SELECT 'Silver', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Silver' LOCK IN SHARE MODE;
        """
    )

    count_gold = MySqlOperator(
        task_id='count_gold',
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (medal_type, count)
        SELECT 'Gold', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Gold' LOCK IN SHARE MODE;
        """
    )

    # 6. Завдання випадкової затримки (5 або 35 секунд)
    def _random_delay():
        delay_seconds = random.choice([5, 35])
        logging.info(f"Sleeping for {delay_seconds} seconds...")
        time.sleep(delay_seconds)

    delay_task = PythonOperator(
        task_id='delay_task',
        python_callable=_random_delay,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # 7. Перевірка сенсором, чи є свіже оновлення в таблиці
    check_recent_record = SqlSensor(
        task_id='check_recent_record',
        conn_id=MYSQL_CONN_ID,
        poke_interval=5,
        timeout=60,
        mode='poke',
        sql=f"""
        SELECT 
            CASE
                WHEN TIMESTAMPDIFF(SECOND, IFNULL(created_at, NOW()), NOW()) <= 30 THEN 1
                ELSE 0
            END
        FROM {TABLE_NAME}
        ORDER BY id DESC
        LIMIT 1;
        """
    )

    # -------------------------------
    # Ланцюжок залежностей DAG
    # -------------------------------
    create_database >> create_medals_table >> pick_medal >> branch
    branch >> [count_bronze, count_silver, count_gold] >> delay_task >> check_recent_record
