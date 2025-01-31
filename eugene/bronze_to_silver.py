from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import re

def clean_text(text):
    # Видалення небажаних символів, залишаючи тільки літери, цифри та основні розділові знаки
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

# Створення користувацької функції (UDF) для очистки тексту
clean_text_udf = udf(clean_text, StringType())

def process_table(spark, table_name):
    # Зчитування даних з папки bronze/{table_name} у форматі Parquet
    df = spark.read.parquet(f"bronze/{table_name}")
    
    # Пошук усіх текстових колонок (тип StringType) у датафреймі
    text_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
    
    # Застосування функції очистки тексту до кожної текстової колонки
    for col_name in text_cols:
        df = df.withColumn(col_name, clean_text_udf(col(col_name)))
    
    # Видалення дублікатів рядків
    df = df.dropDuplicates()
    
    # Вивід очищеного DataFrame у логи для перегляду
    print(f"Data preview after cleaning for {table_name}:")
    df.show()  # Показати очищені дані
    
    # Запис очищених даних у форматі Parquet у папку silver/{table_name} з режимом перезапису
    df.write.parquet(f"silver/{table_name}", mode="overwrite")
    print(f"Processed {table_name} to silver")

if __name__ == "__main__":
    # SparkSession з ім'ям додатку "BronzeToSilver"
    spark = SparkSession.builder \
        .appName("BronzeToSilver") \
        .getOrCreate()

    # Список таблиць для обробки
    tables = ["athlete_bio", "athlete_event_results"]
    
    # Обробка таблиць та виклик функції
    for table in tables:
        process_table(spark, table)
    
    # Завершення сесії
    spark.stop()