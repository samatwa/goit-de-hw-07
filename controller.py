import random
import time
from airflow import DAG
from datetime import datetime
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


def pick_medal():
    """Randomly selects a medal type."""
    return random.choice(["Bronze", "Silver", "Gold"])


def determine_medal_task(ti):
    """Determines the next task based on the selected medal."""
    medal = ti.xcom_pull(task_ids="pick_medal")
    return f"calc_{medal}"


def introduce_delay():
    """Introduces a different delay."""
    delay_time = random.choice([10, 35])  # 10 seconds for success, 35 — for failure
    print(f"Introducing delay of {delay_time} seconds")
    time.sleep(delay_time)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 9),
}

connection_name = "goit_mysql_db_tet_s"

medal_queries = {
    "Bronze": """
        INSERT INTO neo_data.hw_tet_s (medal_type, count, created_at)
        SELECT medal, COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze'
        GROUP BY medal;
    """,
    "Silver": """
        INSERT INTO neo_data.hw_tet_s (medal_type, count, created_at)
        SELECT medal, COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver'
        GROUP BY medal;
    """,
    "Gold": """
        INSERT INTO neo_data.hw_tet_s (medal_type, count, created_at)
        SELECT medal, COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold'
        GROUP BY medal;
    """,
}

with DAG(
    "goit_de_hw7_tet_s",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["tet_s"],
) as dag:

    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS neo_data.hw_tet_s (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME
        );
        """,
    )

    pick_medal_task = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    branch_task = BranchPythonOperator(
        task_id="branch_medal_task",
        python_callable=determine_medal_task,
    )

    medal_tasks = {
        medal: MySqlOperator(
            task_id=f"calc_{medal}",
            mysql_conn_id=connection_name,
            sql=query,
        )
        for medal, query in medal_queries.items()
    }

    delay_task = PythonOperator(
        task_id="introduce_delay",
        python_callable=introduce_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    validation_task = SqlSensor(
        task_id="validate_entry",
        conn_id=connection_name,
        sql="""
        SELECT TIMESTAMPDIFF(SECOND, created_at, NOW()) < 30
        FROM neo_data.hw_tet_s
        ORDER BY id DESC
        LIMIT 1;
        """,
        mode="poke",
        poke_interval=5,
        timeout=31,
    )

    create_table >> pick_medal_task >> branch_task
    for medal_task in medal_tasks.values():
        branch_task >> medal_task >> delay_task >> validation_task
