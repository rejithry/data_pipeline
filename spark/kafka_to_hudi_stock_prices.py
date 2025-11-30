from pyspark.sql import SparkSession

from pyspark.sql.functions import col, from_json

spark = SparkSession.builder.getOrCreate()

with open('/opt/kafka/config/stock_price_schema.avsc','r') as avro_file:
    avro_schema = avro_file.read()

df = spark\
    .read\
    .format("avro")\
    .option("avroSchema", avro_schema)\
    .load()


hive_table_name = "stock_prices"  # Replace with your Hive table name
checkpoint_location = "hdfs://namenode:9000/tmp/checkpoint/kafka_to_hive" # Replace with a suitable HDFS path
hudi_table_location= "hdfs://namenode:9000/stock_prices"



kafka_df = spark \
.readStream \
.format("kafka") \
.option("avroSchema", avro_schema) \
.option("kafka.bootstrap.servers", "kafkabroker:9092") \
.option("subscribe", "stock_prices") \
.option("startingOffsets", "earliest") \
.load()


parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), df.schema).alias("data")) \
        .select("data.*")


db_name = "hudidb"
table_name = "stock_prices2"
recordkey = 'symbol'
precombine = 'ts'
path = "hdfs://namenode:9000/stock_prices_2"
method = 'upsert'
table_type = "COPY_ON_WRITE"
BOOT_STRAP = "kafkabroker:9092"
TOPIC = "stock_prices"
hudi_options = {
    'hoodie.table.name': table_name,
    'hoodie.datasource.write.recordkey.field': 'id',
    'hoodie.datasource.write.table.name': table_name,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'timestamp',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2
}


def process_batch_message(kafka_df, batch_id):
    clean_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), df.schema).alias("data")) \
        .select("data.*")
    if clean_df.count() >0:
        clean_df.write.format("hudi"). \
            options(**hudi_options). \
            mode("append"). \
            save(path)
    print("batch_id : ", batch_id, clean_df.show(truncate=False))

query = kafka_df.writeStream \
    .foreachBatch(process_batch_message) \
    .option("checkpointLocation", checkpoint_location) \
    .trigger(processingTime="1 minutes") \
    .start().awaitTermination()


def write_to_hive(batch_df, batch_id):
    (batch_df
        .write
        .mode("append")
        .format("hive")
        .saveAsTable(hive_table_name)
    )

query = (parsed_df.writeStream
         .outputMode("append")
         .option("checkpointLocation", checkpoint_location)
         .foreachBatch(write_to_hive)
         .start()
        )

query.awaitTermination()
