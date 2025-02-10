import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    # Зчитування даних з бронзового шару
    athlete_bio_df = spark.read.parquet("bronze/athlete_bio")
    athlete_event_results_df = spark.read.parquet("bronze/athlete_event_results")

    # Очищення текстових колонок
    for df in [athlete_bio_df, athlete_event_results_df]:
        for col_name in df.columns:
            df = df.withColumn(col_name, regexp_replace(col(col_name), r'[^a-zA-Z0-9,.\\"\' ]', ''))

    # Дедублікація
    athlete_bio_df = athlete_bio_df.dropDuplicates()
    athlete_event_results_df = athlete_event_results_df.dropDuplicates()

    # Запис у срібний шар
    athlete_bio_df.write.mode("overwrite").parquet("silver/athlete_bio")
    athlete_event_results_df.write.mode("overwrite").parquet("silver/athlete_event_results")

    spark.stop()