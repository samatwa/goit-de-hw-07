from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re

spark = SparkSession.builder \
    .appName("bronze_to_silver") \
    .getOrCreate()


table_list = ['athlete_bio', 'athlete_event_results']


def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\"\']', '', str(text))


clean_text_udf = udf(clean_text, StringType())

for table in table_list:
    print(f"Working on {table} table!")
    df = spark.read.parquet(f"bronze/{table}", header=True, inferSchema=True)

    for col_name, col_type in df.dtypes:
        if col_type == 'string':
            print(f"Cleaning column: {col_name}, Type: {col_type}")
            df = df.withColumn("cleaned_text", clean_text_udf(df[col_name]))

    df = df.drop_duplicates()

    df.coalesce(1).write \
        .mode('overwrite') \
        .parquet(f"silver/{table}")

# Завершення сесії Spark
spark.stop()
