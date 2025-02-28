from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re
import os


# Функція чистки тексту: видаляє небажані символи
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))


# Основна функція для обробки таблиці з bronze до silver
def process_table(table):
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    input_path = f"bronze/{table}"
    output_path = f"silver/{table}"

    # Зчитування даних із bronze
    df = spark.read.parquet(input_path)

    # Отримання імен текстових колонок
    text_columns = [field.name for field in df.schema.fields if field.dataType.simpleString() == "string"]

    # Реєстрація UDF для чистки тексту
    clean_text_udf = udf(clean_text, StringType())
    for col_name in text_columns:
        df = df.withColumn(col_name, clean_text_udf(df[col_name]))

    # Видалення дублікатів
    df_cleaned = df.dropDuplicates()

    # Створення директорії для silver (якщо не існує)
    os.makedirs(output_path, exist_ok=True)

    # Запис очищених даних у Parquet
    df_cleaned.write.mode("overwrite").parquet(output_path)
    print(f"Successfully processed {table} and saved as {output_path}")

    # Вивід даних для перевірки
    df_result = spark.read.parquet(output_path)
    df_result.show(truncate=False)

    spark.stop()


tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    process_table(table)
