from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder \
    .appName("JDBCToKafka") \
    .getOrCreate()

schema = StructType(
    [StructField("tttt", IntegerType(), True)
    ])

df = spark.createDataFrame([(1)], schema=schema)
df.show()


