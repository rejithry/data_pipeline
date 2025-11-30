FROM jupyter/pyspark-notebook:latest

ENV PYSPARK_SUBMIT_ARGS='--packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.postgresql:postgresql:42.7.8,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog pyspark-shell'

COPY *.jar /opt/spark/jars/
COPY kafka-source.properties /var/demo/config/kafka-source.properties
COPY schema.avsc /opt/kafka/config/schema.avsc
COPY kafka_to_hudi.py /tmp/kafka_to_hudi.py
