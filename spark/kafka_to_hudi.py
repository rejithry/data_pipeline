from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.functions import col, from_json  # type: ignore

from config_stock_prices import table_name, recordkey, path, method, schema, broker, topic, checkpoint_location, hudi_table_location

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

with open(schema, "r") as avro_file:
    avro_schema = avro_file.read()

df = spark.read.format("avro").option("avroSchema", avro_schema).load()

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", broker)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()
)

hudi_options = {
    "hoodie.table.name": table_name,
    "hoodie.datasource.write.recordkey.field": recordkey,
    "hoodie.datasource.write.table.name": table_name,
    "hoodie.datasource.write.operation": method,
    "hoodie.upsert.shuffle.parallelism": "2",
    "hoodie.insert.shuffle.parallelism": "2",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.datasource.meta.sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
}

def write_to_postgres(df, batch_id):
    postgres_url = "jdbc:postgresql://postgres:5432/default"
    postgres_table = "stock_prices"
    postgres_user = "admin"
    postgres_password = "admin"

    df.write \
        .format("jdbc") \
        .option("url", postgres_url) \
        .option("dbtable", postgres_table) \
        .option("user", postgres_user) \
        .option("password", postgres_password) \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", 10) \
        .mode("append") \
        .save()
    print("batch_id : ", batch_id)

def process_batch_message(kafka_df, batch_id):
    clean_df = (
        kafka_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), df.schema).alias("data"))
        .select("data.*")
    )
    
    if clean_df.count() > 0:
        clean_df.write.format("hudi").options(**hudi_options).mode("append").save(path)
        write_to_postgres(clean_df, batch_id)
    print("batch_id : ", batch_id)
    clean_df.show(truncate=False)

query = (
    kafka_df.writeStream.foreachBatch(process_batch_message)
    .option("checkpointLocation", checkpoint_location)
    .trigger(processingTime="1 minutes")
    .start()
    .awaitTermination()
)
