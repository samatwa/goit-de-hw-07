from pyspark.sql import SparkSession
import os
import requests


# Завантаження CSV-файлів з FTP-сервера у landing zone
def download_data(local_file_path):
    base_url = "https://ftp.goit.study/neoversity/"
    downloading_url = base_url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    if response.status_code == 200:
        with open(local_file_path + ".csv", 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}.csv")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")


# Читання CSV файлу за допомогою Spark та збереження у форматі Parquet у директорію bronze/{table}
def process_table(table):
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    local_path = f"{table}.csv"
    output_path = f"bronze/{table}"

    # Читання CSV з інтерпретацією заголовків та схеми
    df = spark.read.csv(local_path, header=True, inferSchema=True)

    # Створення директорії для bronze (якщо не існує)
    os.makedirs(output_path, exist_ok=True)

    # Запис у форматі Parquet (перезапис при повторному запуску)
    df.write.mode("overwrite").parquet(output_path)
    print(f"Successfully processed {table} and saved as {output_path}")

    # Вивід даних для перевірки
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

    spark.stop()


tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    download_data(table)
    process_table(table)
