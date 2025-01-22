from datetime import datetime
import random
import time

# Імпорти Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.exceptions import AirflowSkipException

# Аргументи за замовчуванням
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0
}

# Параметри
MYSQL_CONN_ID = 'goit_mysql_db'   # Назва MySQL-з'єднання в Airflow
TABLE_NAME = 'oleksandr_s.medals_log'  # Таблиця, куди пишемо підрахунки

with DAG(
    dag_id='medals_dag_oleks_s',                  # унікальне ім'я DAG
    description='DAG for medals counting with random branching',
    default_args=default_args,
    schedule_interval=None,                    # Вмикаємо ручний запуск
    catchup=False,
    tags=['oleksandr_s']                      # Тег
) as dag:

    # 1. Створення таблиці з потрібними полями (IF NOT EXISTS)
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

    # 2. Завдання: випадковий вибір одного з трьох типів медалей
    def _choose_medal(**context):
        medal_options = ['Bronze', 'Silver', 'Gold']
        chosen = random.choice(medal_options)
        context['ti'].xcom_push(key='picked_medal', value=chosen)

    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=_choose_medal
    )

    # 3. Розгалуження (BranchPythonOperator) залежно від обраної медалі
    def _branch_medal(**context):
        chosen = context['ti'].xcom_pull(key='picked_medal', task_ids='pick_medal')
        if chosen == 'Bronze':
            return 'count_bronze'
        elif chosen == 'Silver':
            return 'count_silver'
        elif chosen == 'Gold':
            return 'count_gold'
        else:
            # На випадок непередбаченого значення
            raise AirflowSkipException("Невідома медаль!")

    branch = BranchPythonOperator(
        task_id='branch_on_medal',
        python_callable=_branch_medal
    )

    # 4. Три завдання: підрахувати кількість відповідних медалей у athlete_event_results
    #    і записати результат у нашу таблицю TABLE_NAME
    count_bronze = MySqlOperator(
        task_id='count_bronze',
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (medal_type, count)
        SELECT 'Bronze', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """
    )

    count_silver = MySqlOperator(
        task_id='count_silver',
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (medal_type, count)
        SELECT 'Silver', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """
    )

    count_gold = MySqlOperator(
        task_id='count_gold',
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (medal_type, count)
        SELECT 'Gold', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """
    )

    # 5. Завдання затримки: випадково обираємо 5 чи 35 секунд
    def _random_delay():
        delay_seconds = random.choice([5, 35])
        print(f"Sleeping for {delay_seconds} seconds...")
        time.sleep(delay_seconds)

    delay_task = PythonOperator(
        task_id='delay_task',
        python_callable=_random_delay
    )

    # 6. Сенсор: перевірка, що найновіший запис у TABLE_NAME не старший за 30 секунд
    #    Якщо умовне поле (CASE) = 5, сенсор завершується успішно, інакше — продовжує «покати» до timeout
    check_recent_record = SqlSensor(
        task_id='check_recent_record',
        conn_id=MYSQL_CONN_ID,
        poke_interval=5,  # Перевірка кожні 5 секунд
        timeout=60,       # Макс. 60 секунд загалом
        mode='poke',
        sql=f"""
        SELECT 
            CASE
                WHEN TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30 THEN 1
                ELSE 0
            END
        FROM {TABLE_NAME}
        ORDER BY id DESC
        LIMIT 1;
        """
    )

    # -------------------------------
    # Ланцюжок залежностей
    # -------------------------------
    create_medals_table >> pick_medal >> branch

    branch >> count_bronze
    branch >> count_silver
    branch >> count_gold

    [count_bronze, count_silver, count_gold] >> delay_task >> check_recent_record
