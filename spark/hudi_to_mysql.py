from pyspark.sql import SparkSession  # type: ignore

from config_stock_prices import hudi_table_location

spark = SparkSession.builder.getOrCreate()

df = spark.read.format("hudi").load(hudi_table_location)



# write dataframe to MySQL
df.write.format("jdbc").options(
    url="jdbc:mysql://mysql:3306/mysql",
    driver="com.mysql.jdbc.Driver",
    dbtable="stock_prices",
    user="mysql",
    password="mysql",
).mode("append").save()
