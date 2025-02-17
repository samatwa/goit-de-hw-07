from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp

if __name__ == "__main__":
    # Створення Spark сесії
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    # Зчитування даних з срібного шару
    athlete_bio_df = spark.read.parquet("silver/athlete_bio")
    athlete_event_results_df = spark.read.parquet("silver/athlete_event_results")

    # Об'єднання даних за athlete_id
    joined_df = athlete_bio_df.join(athlete_event_results_df, "athlete_id")

    # Вибір колонок, щоб уникнути неоднозначності
    joined_df = joined_df.select(
        athlete_bio_df["athlete_id"],
        athlete_bio_df["sex"],
        athlete_bio_df["country_noc"],
        athlete_event_results_df["sport"],
        athlete_event_results_df["medal"],
        athlete_bio_df["weight"],
        athlete_bio_df["height"]
    )

    # Видалення рядків з null-значеннями у важливих колонках
    joined_df = joined_df.dropna(subset=["sport", "medal", "sex", "country_noc", "weight", "height"])

    # Обчислення середніх значень
    avg_stats_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
 .agg(
            avg("weight").alias("average_weight"),
            avg("height").alias("average_height"),
            current_timestamp().alias("timestamp")
        )

    # Запис у золотий шар
    avg_stats_df.write.mode("overwrite").parquet("gold/avg_stats")

    # Завершення сесії Spark
    spark.stop()