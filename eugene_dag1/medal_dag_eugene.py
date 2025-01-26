from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.sql import SqlSensor
from datetime import datetime, timedelta
import random
import time

# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4),
}

# 1. Функція для вибору та запису медалі в XCom
def pick_medal_value(ti):
    chosen_medal = random.choice(['Bronze', 'Silver', 'Gold'])
    print(f"Picked medal: {chosen_medal}")
    ti.xcom_push(key='picked_medal', value=chosen_medal)

# 2. Функція визначає, яку з трьох гілок запускати
def pick_branch(ti):
    chosen_medal = ti.xcom_pull(key='picked_medal', task_ids='pick_medal')
    if chosen_medal == 'Bronze':
        return 'calc_Bronze'
    elif chosen_medal == 'Silver':
        return 'calc_Silver'
    else:
        return 'calc_Gold'

# 3. Функція для створення (випадкової) затримки
def generate_delay():
    # Випадкова затримка: 5 або 35 секунд
    delay = random.choice([5, 35])
    print(f"Sleeping for {delay} seconds...")
    time.sleep(delay)

# 4. SQL для сенсора: перевірка, чи останній запис «не старший за 30 с»
check_recent_record_sql = """
SELECT TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30 
FROM neo_data.hw_dag_results
ORDER BY created_at DESC 
LIMIT 1;
"""

#  DAG
with DAG(
    'medal_dag_eugene',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["eugene"]
) as dag:

    # (1) Створення таблиці
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='goit_mysql_db_eugene',
        sql="""
        CREATE TABLE IF NOT EXISTS neo_data.hw_dag_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(50),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # (2) Випадковий вибір медалі
    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal_value
    )

    # (3) Гілкування
    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=pick_branch
    )

    # (4) Підрахунок Bronze
    calc_Bronze = SQLExecuteQueryOperator(
        task_id='calc_Bronze',
        conn_id='goit_mysql_db_eugene',
        sql="""
        INSERT INTO neo_data.hw_dag_results (medal_type, count)
        SELECT 'Bronze', COUNT(*) 
        FROM neo_data.athlete_event_results
        WHERE medal = 'Bronze';
        """
    )

    # (4) Підрахунок Silver
    calc_Silver = SQLExecuteQueryOperator(
        task_id='calc_Silver',
        conn_id='goit_mysql_db_eugene',
        sql="""
        INSERT INTO neo_data.hw_dag_results (medal_type, count)
        SELECT 'Silver', COUNT(*) 
        FROM neo_data.athlete_event_results
        WHERE medal = 'Silver';
        """
    )

    # (4) Підрахунок Gold
    calc_Gold = SQLExecuteQueryOperator(
        task_id='calc_Gold',
        conn_id='goit_mysql_db_eugene',
        sql="""
        INSERT INTO neo_data.hw_dag_results (medal_type, count)
        SELECT 'Gold', COUNT(*) 
        FROM neo_data.athlete_event_results
        WHERE medal = 'Gold';
        """
    )

    # (5) Затримка (випадково 5с або 35с)
    generate_delay_task = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay,
    )

    # (6) Сенсор - перевірка свіжості останнього запису
    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id='goit_mysql_db_eugene',
        sql=check_recent_record_sql,
        mode='poke',        # або 'reschedule'
        poke_interval=5,    # перевірка кожні 5 секунд
        timeout=30          # загальний таймаут 30 секунд
    )

    # Зв’язки між завданнями
    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> calc_Bronze >> generate_delay_task
    pick_medal_task >> calc_Silver >> generate_delay_task
    pick_medal_task >> calc_Gold >> generate_delay_task
    generate_delay_task >> check_for_correctness