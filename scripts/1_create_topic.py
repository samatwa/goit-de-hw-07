from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
import os

# Налаштування DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 22),
    'retries': 1,
}

dag = DAG(
    'create_kafka_topics_dag_churylov_123',
    default_args=default_args,
    schedule_interval=None,  # Запуск вручну
    catchup=False
)

# Функція для створення Kafka-топіків
def create_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        security_protocol="PLAINTEXT"
    )
    topics = [
        NewTopic(name="athlete_event_results_churylov_123", num_partitions=2, replication_factor=1),
        NewTopic(name="enriched_athlete_avg_churylov_123", num_partitions=2, replication_factor=1)
    ]
    try:
        admin_client.create_topics(new_topics=topics)
        print("Kafka topics created successfully!")
    except KafkaError as ke:
        print(f"Kafka error: {ke}")
    finally:
        admin_client.close()

# Оператор для створення топіків
create_topics_task = PythonOperator(
    task_id='create_kafka_topics',
    python_callable=create_topics,
    dag=dag
)

# Визначаємо DAG
create_topics_task
