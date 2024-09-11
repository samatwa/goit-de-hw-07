from pyspark.sql import SparkSession

import requests

table_list = ['games', 'athlete_bio', 'athlete_event_results']


def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open the local file in write-binary mode and write the content of the response to it
        with open(local_file_path + ".csv", 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")

[download_data(filename) for filename in table_list]

spark = SparkSession.builder \
    .appName("landing_to_bronze") \
    .getOrCreate()

for table in table_list:
    print(f"Working on {table} table!")
    df = spark.read.csv(f"{table}.csv", header=True, inferSchema=True)

    df.show()

    df.coalesce(1).write \
        .mode('overwrite') \
        .parquet(f"bronze/{table}")

# Завершення сесії Spark
spark.stop()
