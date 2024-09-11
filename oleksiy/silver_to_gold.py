from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp

spark = SparkSession.builder \
    .appName("silver_to_gold") \
    .getOrCreate()

athlete_bio = spark.read.parquet(f"silver/athlete_bio", header=True, inferSchema=True)
athlete_event_results = spark.read.parquet(f"silver/athlete_event_results", header=True, inferSchema=True)

event_stream_enriched = athlete_event_results.join(athlete_bio, "athlete_id", "inner") \
    .drop(athlete_bio.country_noc) \
    .where("weight != 'nan' AND height!= ''") \
    .groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight")
) \
    .withColumn("timestamp", current_timestamp())

event_stream_enriched.coalesce(1).write \
        .mode('overwrite') \
        .parquet(f"gold/avg_stats")

spark.stop()
