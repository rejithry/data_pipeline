cd spark
docker build -t jupyter_notebook .

docker compose up -d

sleep 5

cat data/batch_1.json | kcat -b kafkabroker -t stock_ticks -P

docker exec -it spark_test spark-submit \
   --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:1.0.1,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.1  \
   --conf hoodie.streamer.schemaprovider.source.schema.file=/opt/kafka/config/schema.avsc \
   --conf spark.kafka.clusters.mycluster.auth.bootstrap.servers="workspace-kafkabroker-1:9092" \
   --class org.apache.hudi.utilities.streamer.HoodieStreamer \
    /opt/spark/jars/hudi-utilities-slim-bundle_2.12-1.0.1.jar \
    --table-type COPY_ON_WRITE   --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
    --source-ordering-field ts    --target-base-path /user/hive/warehouse/stock_ticks_cow \
    --target-table stock_ticks_cow --props /var/demo/config/kafka-source.properties \
    --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider  
