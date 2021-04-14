"""
export SPARK_KAFKA_VERSION=0.10
/spark2.4/bin/pyspark \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 \
    --driver-memory 512m \
    --num-executors 1 \
    --executor-memory 512m \
    --master local[1]
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, StructField

spark = SparkSession.builder.appName("gogin_spark").getOrCreate()

kafka_brokers = "10.0.0.6:6667"

enrollee_schema = StructType([StructField('c0', StringType()),
                              StructField('enrollee_id', StringType()),
                              StructField('datetime', StringType())])

enrollee_id_df = spark.read \
    .options(delimiter=',', schema=enrollee_schema, header=True) \
    .csv(path="input_csv_for_stream/enrollee_id_for_kafka.csv") \
    .select('enrollee_id', 'datetime')

enrollee_id_df \
    .selectExpr("CAST(null AS STRING) as key", "CAST(to_json(struct(*)) AS STRING) as value") \
    .write \
    .format(source="kafka") \
    .option("topic", "enrollee_id") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("checkpointLocation", "checkpoints/enrollee_id_for_kafka") \
    .save()


def console_output(df, freq, truncate=True, numRows=20):
    """
    Вывод на консоль вместо show()
    :param numRows: number of lines to display
    :param df: spark DataFrame
    :param freq: frequency in seconds
    :param truncate: truncate values, bool
    """
    return df.writeStream \
        .format(source="console") \
        .trigger(processingTime='%s seconds' % freq) \
        .options(truncate=truncate, numRows=numRows) \
        .start()


# Проверка, прочитали потихоньку
raw_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", "enrollee_id") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "1") \
    .load() \
    .select(F.col("value").cast("String"), "offset") \
    .select(F.from_json(F.col("value"), enrollee_schema).alias("value"), "offset") \
    .select("value.*", "offset") \
    .select('enrollee_id', 'datetime', 'offset')

out = console_output(df=raw_data, freq=5, truncate=False)
out.stop()
