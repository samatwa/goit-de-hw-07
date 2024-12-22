from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp
import os

# Налаштування DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 22),
    'retries': 1,
}

dag = DAG(
    'streaming_kafka_mysql_dag_churylov_123',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# Ініціалізація Spark
def get_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

# Завантаження даних із MySQL
def read_from_mysql():
    spark = get_spark_session("MySQLReader")
    jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
    properties = {
        "user": "neo_data_admin",
        "password": "Proyahaxuqithab9oplp",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    df = spark.read.jdbc(url=jdbc_url, table="athlete_event_results", properties=properties)
    df.write.mode("overwrite").parquet("/tmp/mysql_data")
    spark.stop()

# Фільтрація даних та запис у Kafka
def write_to_kafka():
    spark = get_spark_session("KafkaWriter")
    df = spark.read.parquet("/tmp/mysql_data")
    df = df.filter("medal IS NOT NULL AND sport IS NOT NULL")
    df.selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value") \
        .write.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "athlete_event_results_churylov_123") \
        .save()
    spark.stop()

# Агрегація даних
def aggregate_data():
    spark = get_spark_session("Aggregator")
    df = spark.read.parquet("/tmp/mysql_data")
    aggregated = df.groupBy("sport", "medal").agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("timestamp")
    )
    aggregated.write.mode("overwrite").parquet("/tmp/aggregated_data")
    spark.stop()

# Запис у MySQL
def write_to_mysql():
    spark = get_spark_session("MySQLWriter")
    df = spark.read.parquet("/tmp/aggregated_data")
    jdbc_url = "jdbc:mysql://217.61.57.46:3306/neo_data"
    properties = {
        "user": "neo_data_admin",
        "password": "Proyahaxuqithab9oplp",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    df.write.jdbc(url=jdbc_url, table="enriched_athlete_avg_churylov_123", mode="append", properties=properties)
    spark.stop()

# Завдання в DAG
read_mysql_task = PythonOperator(
    task_id='read_from_mysql',
    python_callable=read_from_mysql,
    dag=dag
)

write_kafka_task = PythonOperator(
    task_id='write_to_kafka',
    python_callable=write_to_kafka,
    dag=dag
)

aggregate_data_task = PythonOperator(
    task_id='aggregate_data',
    python_callable=aggregate_data,
    dag=dag
)

write_mysql_task = PythonOperator(
    task_id='write_to_mysql',
    python_callable=write_to_mysql,
    dag=dag
)

# Зв'язок між завданнями
read_mysql_task >> write_kafka_task >> aggregate_data_task >> write_mysql_task
