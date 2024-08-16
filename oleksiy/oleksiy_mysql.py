from airflow import DAG
#from airflow.providers.mysql.operators.mysql import MySqlOperator
#from airflow.providers.mysql.sensors.mysql import MySqlSensor
from datetime import datetime
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator

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
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        SELECT * FROM olympic_dataset.athlete_bio LIMIT 1;
        """
    )

    # Завдання для вставки даних в таблицю
    insert_data = MySqlOperator(
        task_id='insert_data',
        mysql_conn_id=connection_name,
        sql="""
        SELECT * FROM olympic_dataset.athlete_bio LIMIT 1;
        """
    )

    # Сенсор для перевірки наявності даних у таблиці
    check_for_data = MySqlSensor(
        task_id='check_for_data',
        mysql_conn_id=connection_name,
        sql="SELECT COUNT(1) FROM olympic_dataset.athlete_bio;",
        mode='poke',  # Режим очікування (poke або reschedule)
        timeout=300  # Час очікування в секундах
    )

    # Завдання для вибору даних з таблиці
    select_data = MySqlOperator(
        task_id='select_data',
        mysql_conn_id='my_mysql_conn_id',
        sql="SELECT * FROM olympic_dataset.athlete_bio LIMIT 1;",
    )

    # Встановлення залежностей
    create_table >> insert_data >> check_for_data >> select_data
