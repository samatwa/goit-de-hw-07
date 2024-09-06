import random
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PiCalculation").getOrCreate()
sc = spark.sparkContext

def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

num_samples = 1000000
count = sc.parallelize(range(0, num_samples)).filter(inside).count()
print("Pi is roughly %f" % (4.0 * count / num_samples))

spark.stop()
