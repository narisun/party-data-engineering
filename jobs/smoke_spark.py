from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print("SMOKE_OK")
spark.range(3).show()
spark.stop()
