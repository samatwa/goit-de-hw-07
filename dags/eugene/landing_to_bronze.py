import requests
from pyspark.sql import SparkSession
import os

def download_data(table_name):
    # URL для завантаження CSV-файлу таблиці
    url = f"https://ftp.goit.study/neoversity/{table_name}.csv"
    print(f"Downloading from {url}")
    
    # HTTP GET запит для завантаження файлу
    response = requests.get(url)

    # Статус відповіді
    if response.status_code == 200:
        # Відкриття локального файлу для запису в бінарному режимі
        with open(f"{table_name}.csv", 'wb') as f:
            f.write(response.content)  # Запис відповіді у файл
        print(f"Downloaded {table_name}.csv")
    else:
        # Винятки
        raise Exception(f"Failed to download {table_name}.csv")

def process_table(spark, table_name):
    # CSV-файл таблиці
    download_data(table_name)
    
    # Читання CSV-файлу за допомогою Spark з автоматичним визначенням схеми та заголовками колонок
    df = spark.read.csv(f"{table_name}.csv", header=True, inferSchema=True)
    
    # Запис даних у форматі Parquet у папку bronze/{table_name} з режимом перезапису
    df.write.parquet(f"bronze/{table_name}", mode="overwrite")
    print(f"Processed {table_name} to bronze")

if __name__ == "__main__":
    # SparkSession з ім'ям додатку "LandingToBronze"
    spark = SparkSession.builder \
        .appName("LandingToBronze") \
        .getOrCreate()

    # Список таблиць для обробки
    tables = ["athlete_bio", "athlete_event_results"]
    
    # Обробка таблиць та виклик функції
    for table in tables:
        process_table(spark, table)
    
    # Завершення сесії
    spark.stop()