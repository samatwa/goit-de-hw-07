from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
import random
import time

# Аргументи за замовчуванням
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4),
}

# 1) Випадковий вибір медалі
def pick_medal_value(**kwargs):
    chosen_medal = random.choice(['Bronze', 'Silver', 'Gold'])
    print(f"Picked medal: {chosen_medal}")
    # Записуємо в XCom
    kwargs['ti'].xcom_push(key='picked_medal', value=chosen_medal)

# 2) Функція визначає, яку з трьох гілок запускати
def pick_branch(**kwargs):
    chosen_medal = kwargs['ti'].xcom_pull(task_ids='pick_medal', key='picked_medal')
    if chosen_medal == 'Bronze':
        return 'calc_Bronze'
    elif chosen_medal == 'Silver':
        return 'calc_Silver'
    else:
        return 'calc_Gold'

# 3) Затримка (рандомно 5 або 35 секунд)
def generate_delay():
    delay = random.choice([5, 35])
    print(f"Sleeping for {delay} seconds...")
    time.sleep(delay)

# 4) Запит для сенсора: перевірити, чи останній запис не старший за 30 секунд
check_recent_record_sql = """
SELECT TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30 
FROM olympic_dataset.eugene_medal_counts
ORDER BY created_at DESC 
LIMIT 1;
"""

# Створюємо DAG
with DAG(
    'medal_dag_eugene',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["eugene"]
) as dag:

    # (1) Створення таблиці
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='goit_mysql_db_eugene',
        sql="""
        CREATE TABLE IF NOT EXISTS olympic_dataset.eugene_medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(50),
            count INT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # (2) Обрати випадкову медаль
    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal_value,
        provide_context=True
    )

    # (3) Розгалуження (BranchPythonOperator)
    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=pick_branch,
        provide_context=True
    )

    # (4) Підрахунок Bronze
    calc_Bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id='goit_mysql_db_eugene',
        sql="""
        INSERT INTO olympic_dataset.eugene_medal_counts (medal_type, count)
        SELECT 'Bronze', COUNT(*) 
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """
    )

    # (4) Підрахунок Silver
    calc_Silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id='goit_mysql_db_eugene',
        sql="""
        INSERT INTO olympic_dataset.eugene_medal_counts (medal_type, count)
        SELECT 'Silver', COUNT(*) 
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """
    )

    # (4) Підрахунок Gold
    calc_Gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id='goit_mysql_db_eugene',
        sql="""
        INSERT INTO olympic_dataset.eugene_medal_counts (medal_type, count)
        SELECT 'Gold', COUNT(*) 
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """
    )

    # (5) Затримка (5 або 35с)
    generate_delay_task = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay
    )

    # (6) Сенсор - перевіряє, чи останній запис створений < 30 секунд тому
    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id='goit_mysql_db_eugene',
        sql=check_recent_record_sql,
        poke_interval=5,
        timeout=30,
        mode='poke'
    )

    # Зв’язки
    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> calc_Bronze >> generate_delay_task
    pick_medal_task >> calc_Silver >> generate_delay_task
    pick_medal_task >> calc_Gold   >> generate_delay_task
    generate_delay_task >> check_for_correctness