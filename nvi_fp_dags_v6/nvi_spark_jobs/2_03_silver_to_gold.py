from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp
import os

# Створення Spark сесії для перетворення даних із silver у gold
spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

# Зчитування даних з рівня silver
athlete_bio_df = spark.read.parquet("silver/athlete_bio")
athlete_event_results_df = spark.read.parquet("silver/athlete_event_results")

# Виконання join за ключем athlete_id
joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

# Групування за sport, medal, sex, country_noc та обчислення середніх значень weight і height
agg_df = (joined_df.groupBy("sport", "medal", "sex", "country_noc")
          .agg(
              avg("height").alias("avg_height"),
              avg("weight").alias("avg_weight"),
              current_timestamp().alias("timestamp")
          )
)

# Створення директорії для gold (якщо не існує)
output_path = "gold/avg_stats"
os.makedirs(output_path, exist_ok=True)

# Запис результатів у Parquet формат
agg_df.write.mode("overwrite").parquet(output_path)
print(f"Successfully processed data and saved to {output_path}")

# Вивід фінального DataFrame для перевірки
df_final = spark.read.parquet(output_path)
df_final.show(truncate=False)

spark.stop()
