"""
export SPARK_KAFKA_VERSION=0.10
/spark2.4/bin/pyspark \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 \
    --driver-memory 512m \
    --driver-cores 1 \
    --master local[1]
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("gogin_spark").getOrCreate()

# для начала готовим DataFrame
aug_all_df = spark.read \
    .options(delimiter=',', inferschema=True, header=True) \
    .csv(path="input_csv_for_stream/aug_all_for_cassandra.csv") \
    .select('enrollee_id', 'city', 'city_development_index', 'gender', 'relevent_experience',
            'enrolled_university', 'education_level', 'major_discipline', 'experience', 'company_size',
            'company_type', 'last_new_job', 'training_hours', 'target')

# пишем
aug_all_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="aug_all", keyspace="koryagin") \
    .mode("append") \
    .save()

# Проверяем - читаем большой большой датасет по ключу
cass_big_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="aug_all", keyspace="koryagin") \
    .load()

cass_big_df.filter(F.col("enrollee_id") == "27107").show()
