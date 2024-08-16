from airflow import DAG
from datetime import datetime
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator

def mark_dag_success(ti, **kwargs):
    # Get the current DagRun object
    dag_run = kwargs['dag_run']
    
    # Mark the DagRun as successful
    dag_run.set_state(State.SUCCESS)

# Визначення DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

connection_name = "goit_mysql_db"

with DAG(
        'working_with_mysql_db',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["oleksiy"]
) as dag:

    # Завдання для створення таблиці, якщо вона не існує
    create_schema = MySqlOperator(
        task_id='create_schema',
        mysql_conn_id=connection_name,
        sql="""
        CREATE DATABASE IF NOT EXISTS oleksiy;
        """
    )

    # Завдання для вставки даних в таблицю
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS oleksiy.games (
        `edition` text,
        `edition_id` int DEFAULT NULL,
        `edition_url` text,
        `year` int DEFAULT NULL,
        `city` text,
        `country_flag_url` text,
        `country_noc` text,
        `start_date` text,
        `end_date` text,
        `competition_date` text,
        `isHeld` text
        );
        """
    )

    # Сенсор для перевірки наявності даних у таблиці
    check_for_data = SqlSensor(
        task_id='check_if_couns_same',
        conn_id=connection_name,
        sql="""WITH count_in_copy AS (
                select COUNT(*) nrows_copy from oleksiy.games
                ),
                count_in_original AS (
                select COUNT(*) nrows_original from olympic_dataset.games
                )
               SELECT nrows_copy <> nrows_original FROM count_in_copy
               CROSS JOIN count_in_original
               ;""",
        mode='poke',  # Режим очікування (poke або reschedule)
        poke_interval=10,  # Check every 60 seconds
        timeout=11,  # Timeout after 10 minutes
    )

    # Завдання для вибору даних з таблиці
    refresh_data = MySqlOperator(
        task_id='refresh',
        mysql_conn_id=connection_name,
        sql="""
            TRUNCATE oleksiy.games;
            INSERT INTO oleksiy.games SELECT * FROM olympic_dataset.games;
        """,
    )

    mark_success_task = PythonOperator(
        task_id='mark_success',
        trigger_rule='one_failed',
        python_callable=mark_dag_success,
        provide_context=True,
        dag=dag,
    )

    # Встановлення залежностей
    create_schema >> create_table >> check_for_data >> refresh_data
    check_for_data >> mark_success_task
