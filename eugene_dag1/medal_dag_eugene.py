from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.sql import SqlSensor
import random
import time

# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

# Функція для створення затримки
def generate_delay():
    time.sleep(35)  # Затримка 35 секунд

# Функція для випадкового вибору медалі
def pick_medal():
    return random.choice(['calc_Bronze', 'calc_Silver', 'calc_Gold'])

# Функція для перевірки коректності даних
check_recent_record_sql = """
SELECT TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30 
FROM medal_table 
ORDER BY created_at DESC 
LIMIT 1;
"""

# Визначення DAG
with DAG(
    'medal_dag_eugene',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["eugene"]
) as dag:

    # Завдання для створення таблиці
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='goit_mysql_db_eugene',
        sql="""
        CREATE TABLE IF NOT EXISTS medal_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(50),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Завдання для вибору медалі
    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=pick_medal
    )

    # Завдання для підрахунку Bronze
    calc_Bronze = SQLExecuteQueryOperator(
        task_id='calc_Bronze',
        conn_id='goit_mysql_db_eugene',
        sql="""
        INSERT INTO medal_table (medal_type, count)
        SELECT 'Bronze', COUNT(*) 
        FROM neo_data.athlete_event_results
        WHERE medal = 'Bronze';
        """
    )

    # Завдання для підрахунку Silver
    calc_Silver = SQLExecuteQueryOperator(
        task_id='calc_Silver',
        conn_id='goit_mysql_db_eugene',
        sql="""
        INSERT INTO medal_table (medal_type, count)
        SELECT 'Silver', COUNT(*) 
        FROM neo_data.athlete_event_results
        WHERE medal = 'Silver';
        """
    )

    # Завдання для підрахунку Gold
    calc_Gold = SQLExecuteQueryOperator(
        task_id='calc_Gold',
        conn_id='goit_mysql_db_eugene',
        sql="""
        INSERT INTO medal_table (medal_type, count)
        SELECT 'Gold', COUNT(*) 
        FROM neo_data.athlete_event_results
        WHERE medal = 'Gold';
        """
    )

    # Завдання для затримки
    generate_delay_task = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay,
    )

    # Сенсор для перевірки останнього запису
    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id='goit_mysql_db_eugene',
        sql=check_recent_record_sql,
        mode='poke',
        poke_interval=5,
        timeout=30,
    )

    # Встановлення залежностей
    create_table >> pick_medal_task
    pick_medal_task >> calc_Bronze >> generate_delay_task
    pick_medal_task >> calc_Silver >> generate_delay_task
    pick_medal_task >> calc_Gold >> generate_delay_task
    generate_delay_task >> check_for_correctness
