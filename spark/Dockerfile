FROM jupyter/pyspark-notebook:latest

ENV PYSPARK_SUBMIT_ARGS='--packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.postgresql:postgresql:42.7.8,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog pyspark-shell'

SHELL ["/bin/bash", "-c"]

COPY *.jar /opt/spark/jars/
COPY kafka-source.properties /var/demo/config/kafka-source.properties
COPY schema.avsc /opt/kafka/config/schema.avsc
COPY stock_price_schema.avsc /opt/kafka/config/stock_price_schema.avsc
COPY kafka_to_hudi.py /tmp/kafka_to_hudi.py
COPY config_stock_prices.py /tmp/config_stock_prices.py
COPY config_stock_ticks.py /tmp/config_stock_ticks.py
COPY query_hudi_table.py /tmp/query_hudi_table.py
COPY hudi_to_mysql.py /tmp/hudi_to_mysql.py
