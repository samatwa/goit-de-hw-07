from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col

if __name__ == "__main__":
    # SparkSession з ім'ям додатку "SilverToGold"
    spark = SparkSession.builder \
        .appName("SilverToGold") \
        .getOrCreate()

    # Зчитування даних з папок silver/athlete_bio та silver/athlete_event_results у форматі Parquet
    bio_df = spark.read.parquet("silver/athlete_bio")
    results_df = spark.read.parquet("silver/athlete_event_results")

    # Перетворення типу колонок weight та height на Double для коректних математичних операцій
    bio_df = bio_df.withColumn("weight", col("weight").cast("double")) \
                   .withColumn("height", col("height").cast("double"))

    # Вибір необхідних колонок з athlete_bio
    bio_selected = bio_df.select("athlete_id", "sex", "country_noc", "weight", "height")

    # Вибір необхідних колонок з athlete_event_results
    results_selected = results_df.select("athlete_id", "sport", "medal")

    # Об'єднання датафреймів за колонкою athlete_id
    joined_df = bio_selected.join(results_selected, on="athlete_id", how="inner")

    # Групування даних за sport, medal, sex, country_noc та обчислення середніх значень weight і height
    avg_stats = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
            avg("weight").alias("avg_weight"),  # Середня вага
            avg("height").alias("avg_height")   # Середній зріст
        ) \
        .withColumn("timestamp", current_timestamp())  # Колонка з поточним часом

    # Вивід фінального DataFrame у логи для перегляду
    print("Final aggregated data preview:")
    avg_stats.show()  # Показати фінальний результат у консолі

    # Запис результатів у папку gold/avg_stats у форматі Parquet з режимом перезапису
    avg_stats.write.parquet("gold/avg_stats", mode="overwrite")

    # Завершення сесії
    spark.stop()