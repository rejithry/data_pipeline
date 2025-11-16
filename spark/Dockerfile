FROM jupyter/pyspark-notebook:latest

ENV PYSPARK_SUBMIT_ARGS='--packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.postgresql:postgresql:42.7.8,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-shell'

COPY *.jar /opt/spark/jars/
COPY kafka-source.properties /var/demo/config/kafka-source.properties
COPY schema.avsc /opt/kafka/config/schema.avsc