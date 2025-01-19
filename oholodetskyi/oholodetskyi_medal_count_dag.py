import random
import time
from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule as tr
from datetime import datetime

# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 19),
}

# Функція для генерації випадкового значення
def pick_medal():
    medals = ['Bronze', 'Silver', 'Gold']
    selected_medal = random.choice(medals)
    return selected_medal

# Функція для вибору наступного завдання
def decide_task(**kwargs):
    medal = kwargs['ti'].xcom_pull(task_ids='pick_medal')
    return f"calc_{medal}"

# Функція для затримки
def generate_delay():
    time.sleep(35)  # 35 секунд затримки для демонстрації

# Визначення DAG
with DAG(
        'oholodetskyi_medal_count_dag',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["oholodetskyi"]
) as dag:

    # Завдання 1: Створення таблиці
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='goit_mysql_db_oholodetskyi',
        sql="""
        CREATE TABLE IF NOT EXISTS oholodetskyi_medal_count (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Завдання 2: Генерація випадкового значення
    pick_medal_task = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal,
    )

    # Завдання 3: Розгалуження
    branch_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=decide_task,
        provide_context=True,
    )

    # Завдання 4: Рахунок для Bronze
    calc_bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id='goit_mysql_db_oholodetskyi',
        sql="""
        INSERT INTO oholodetskyi_medal_count (medal_type, count, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """
    )

    # Завдання 4: Рахунок для Silver
    calc_silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id='goit_mysql_db_oholodetskyi',
        sql="""
        INSERT INTO oholodetskyi_medal_count (medal_type, count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """
    )

    # Завдання 4: Рахунок для Gold
    calc_gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id='goit_mysql_db_oholodetskyi',
        sql="""
        INSERT INTO oholodetskyi_medal_count (medal_type, count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """
    )

    # Завдання 5: Затримка
    delay_task = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay,
        trigger_rule=tr.ONE_SUCCESS,
    )

    # Завдання 6: Сенсор
    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id='goit_mysql_db_oholodetskyi',
        sql="""
        SELECT CASE
            WHEN TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30 THEN 1
            ELSE 0
        END
        FROM oholodetskyi_medal_count;
        """,
        mode='poke',
        poke_interval=5,
        timeout=60,
    )

    # Встановлення залежностей
    create_table >> pick_medal_task >> branch_task
    branch_task >> [calc_bronze, calc_silver, calc_gold] >> delay_task >> check_for_correctness
