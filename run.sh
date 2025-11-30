cd spark
docker build -t jupyter_notebook .


cd ../

cd client
docker build -t client .

cd ../

docker compose up -d

sleep 5

cat data/batch_1.json | kcat -b kafkabroker -t stock_ticks -P

docker exec -it hadoop hadoop fs -chmod -R 777 /

# docker exec -it spark_test spark-submit --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
#   --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar \
#   --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
#   --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
#    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.1 \
#    /tmp/kafka_to_hudi.py
