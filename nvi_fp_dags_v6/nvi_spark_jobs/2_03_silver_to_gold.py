from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp
import os

# Створення Spark сесії
spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

# Читаємо дані
silver_path_bio = "silver/athlete_bio"
silver_path_results = "silver/athlete_event_results"

if not os.path.exists(silver_path_bio) or not os.path.exists(silver_path_results):
    raise FileNotFoundError("❌ One or more input files are missing in the 'silver' directory.")

athlete_bio_df = spark.read.parquet(silver_path_bio)
athlete_event_results_df = spark.read.parquet(silver_path_results)

# Видаляємо дубльовану колонку перед join
athlete_bio_df = athlete_bio_df.drop("country_noc")

# Виконуємо join
joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

# Видаляємо NULL у height і weight
joined_df = joined_df.dropna(subset=["height", "weight"])

# Групування
agg_df = (joined_df.groupBy("sport", "medal", "sex", "country_noc")
          .agg(
              avg("height").alias("avg_height"),
              avg("weight").alias("avg_weight"),
              current_timestamp().alias("timestamp")
          )
)

# Запис результату
output_path = "gold/avg_stats"
agg_df.write.mode("overwrite").parquet(output_path)
print(f"✅ Successfully processed data and saved to {output_path}")

# Вивід перших 10 записів
df_final = spark.read.parquet(output_path)
df_final.show(10, truncate=True)

# Завершуємо сесію
spark.stop()
