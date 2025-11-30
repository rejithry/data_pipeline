cd spark
docker build -t jupyter_notebook .


cd ../

cd client
docker build -t client .

cd ../

cd superset
docker build -t superset .
docker compose -f docker-compose-superset.yml up -d

cd ../

cd flink-processor
mvn clean package
docker build -t flink_processor .
cd ../

docker compose up -d



# echo "Loading data to Hudi..."
# sleep 10

# docker exec -it spark_test /usr/local/spark/sbin/start-thriftserver.sh  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
#   --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar \
#   --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
#   --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
#   --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
#    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.1

# docker exec -it spark_test spark-submit --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
#   --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar \
#   --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
#   --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
#   --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
#    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.1,org.postgresql:postgresql:42.2.5 \
#    /tmp/kafka_to_hudi.py


# echo "Data load completed."

# echo "Querying the Hudi table..."



# # sleep 3

# docker exec -it spark_test spark-sql --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
#   --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar \
#   --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
#   --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
#   --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
#    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.1 \
#    /tmp/query_hudi_table.py


# docker exec -it spark_test spark-submit --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
#   --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar \
#   --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
#   --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
#   --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
#    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.1,mysql:mysql-connector-java:8.0.33 \
#    /tmp/hudi_to_mysql.py