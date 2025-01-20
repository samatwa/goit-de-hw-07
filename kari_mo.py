from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.sql import SqlSensor
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import random
import time

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

connection_name = "goit_mysql_db_kari"

def choose_medal_type():
    return random.choice(['Bronze', 'Silver', 'Gold'])

def delayed_execution():
    time.sleep(35)

with DAG(
        'kari_mo',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["kari"]
) as dag:

    create_schema = MySqlOperator(
    task_id='create_schema',
    mysql_conn_id=connection_name,
    sql="""
    CREATE DATABASE IF NOT EXISTS kari_mo;
    """
    )

    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS kari_mo.kari_statistics (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(255),
            count INT,
            created_at DATETIME
        );
        """
    )

    choose_medal = BranchPythonOperator(
        task_id='choose_medal',
        python_callable=choose_medal_type
    )

    bronze_task = MySqlOperator(
        task_id='bronze_task',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO kari_mo.kari_statistics (medal_type, count, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """
    )

    silver_task = MySqlOperator(
        task_id='silver_task',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO kari_mo.kari_statistics (medal_type, count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """
    )

    gold_task = MySqlOperator(
        task_id='gold_task',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO kari_mo.kari_statistics (medal_type, count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """
    )

    delay_task = PythonOperator(
        task_id='delay_task',
        python_callable=delayed_execution,
        trigger_rule='one_success',  
    )

    check_recent_record = SqlSensor(
        task_id='check_recent_record',
        conn_id=connection_name,
        sql="""
        SELECT 1 FROM kari_mo.kari_statistics
        WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30
        ORDER BY created_at DESC
        LIMIT 1;
        """,
        mode='poke',
        poke_interval=5,
        timeout=40,
    )

    # Define task dependencies
    create_schema >> create_table >> choose_medal
    choose_medal >> [bronze_task, silver_task, gold_task] >> delay_task >> check_recent_record