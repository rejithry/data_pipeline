from pyspark.sql import SparkSession  # type: ignore

from config_stock_prices import hudi_table_location

spark = SparkSession.builder.getOrCreate()

df = spark.read.format("hudi").load(hudi_table_location)
df.show()