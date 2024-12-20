from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.sql import SqlSensor
from datetime import datetime, timedelta
import random
import time

# Функція для вибору випадкового медалю
def pick_medal():
    return random.choice(['calc_Bronze', 'calc_Silver', 'calc_Gold'])

# Функція для затримки виконання
def generate_delay():
    time.sleep(35)

# Налаштування DAG
with DAG(
    'medal_count_dag_churylov',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG для підрахунку медалей',
    schedule_interval=None,
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=['churylov'],
) as dag:

    # 1. Завдання для створення таблиці
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='your_mysql_connection',
        sql="""
        CREATE TABLE IF NOT EXISTS medals (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # 2. Завдання для вибору медалі
    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=pick_medal
    )

    # 3. Завдання для кожного типу медалі
    calc_Bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id='your_mysql_connection',
        sql="""
        INSERT INTO medals (medal_type, count)
        SELECT 'Bronze', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """,
    )

    calc_Silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id='your_mysql_connection',
        sql="""
        INSERT INTO medals (medal_type, count)
        SELECT 'Silver', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """,
    )

    calc_Gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id='your_mysql_connection',
        sql="""
        INSERT INTO medals (medal_type, count)
        SELECT 'Gold', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """,
    )

    # 4. Завдання для затримки
    generate_delay_task = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay
    )

    # 5. Сенсор для перевірки актуальності запису
    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id='your_mysql_connection',
        sql="""
        SELECT 1
        FROM medals
        WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30
        ORDER BY created_at DESC
        LIMIT 1;
        """,
        timeout=60,
        poke_interval=10,
        mode='poke',
    )

    # Зв’язки між задачами
    create_table >> pick_medal_task
    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay_task
    generate_delay_task >> check_for_correctness