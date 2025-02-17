import requests
from pyspark.sql import SparkSession

def download_data(table_name):
    url = f"https://ftp.goit.study/neoversity/{table_name}.csv"
    print(f"Downloading from {url}")
    
    response = requests.get(url)
    if response.status_code == 200:
        with open(f"{table_name}.csv", 'wb') as file:
            file.write(response.content)
        print(f"File {table_name}.csv downloaded successfully.")
    else:
        exit(f"Failed to download {table_name}. Status code: {response.status_code}")

if __name__ == "__main__":
    # Завантаження файлів
    for table in ["athlete_bio", "athlete_event_results"]:
        download_data(table)

    # Створення Spark сесії
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    # Читання CSV-файлів із явним визначенням схеми
    athlete_bio_df = spark.read.option("header", True).option("inferSchema", True).csv("athlete_bio.csv")
    athlete_event_results_df = spark.read.option("header", True).option("inferSchema", True).csv("athlete_event_results.csv")

    # Запис у форматі Parquet з режимом overwrite
    athlete_bio_df.write.mode("overwrite").parquet("bronze/athlete_bio")
    athlete_event_results_df.write.mode("overwrite").parquet("bronze/athlete_event_results")

    spark.stop()