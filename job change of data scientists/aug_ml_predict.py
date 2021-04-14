"""
export SPARK_KAFKA_VERSION=0.10
/spark2.4/bin/pyspark \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 \
    --driver-memory 512m \
    --driver-cores 1 \
    --master local[1]
"""

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("gogin_spark").getOrCreate()

kafka_brokers = "10.0.0.6:6667"

# читаем кафку по одной записи, но можем и по 1000 за раз
enrollee_schema = StructType([StructField('c0', StringType()),
                              StructField('enrollee_id', StringType()),
                              StructField('datetime', StringType())])

enrollees_df = spark.readStream \
    .format(source="kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", "enrollee_id") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "1") \
    .load() \
    .select(F.col("value").cast("String"), "offset") \
    .select(F.from_json(F.col("value"), enrollee_schema).alias("value"), "offset") \
    .select("value.*", "offset") \
    .select('enrollee_id', 'datetime', 'offset')

enrollees_df.printSchema()


def console_output(df, freq, truncate=True, numRows=20):
    """
    Вывод на консоль вместо show()
    :param df: spark DataFrame
    :param freq: frequency in seconds
    :param truncate: truncate values, bool
    """
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq) \
        .options(truncate=truncate) \
        .options(numRows=numRows) \
        .start()


# out = console_output(df=enrollees_df, freq=5, truncate=False)
# out.stop()

###############
# подготавливаем DataFrame для запросов к касандре с историческими данными
cassandra_features_raw = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="aug_all", keyspace="koryagin") \
    .load()

cassandra_features_selected = cassandra_features_raw.drop("target")
# cassandra_features_selected.show(n=5)

# подгружаем ML из HDFS
pipeline_model = PipelineModel.load(path="ml_models/my_LR_model_enrollees")


##########
# вся логика в этом foreachBatch
def writer_logic(df, epoch_id):
    """ Function for foreachBatch
    :param df: stream DataFrame from kafka
    :param epoch_id: --
    """
    df.persist()
    print("---------I've got new batch--------")
    print("This is what I've got from Kafka:")
    df.show()
    features_from_kafka = df.select("enrollee_id")
    #
    # оставим только уникальные "enrollee_id"
    users_list_df = features_from_kafka.distinct()
    # превращаем DataFrame(Row) в Array(Row)
    users_list_rows = users_list_df.collect()
    # превращаем Array(Row) в Array(String)
    users_list = map(lambda x: str(x.__getattr__("enrollee_id")), users_list_rows)
    # print(users_list)
    where_string = " enrollee_id = " + " or enrollee_id = ".join(users_list)
    print("I'm gonna select this from Cassandra:")
    print(where_string)
    features_from_cassandra = cassandra_features_selected \
        .where(where_string) \
        .na.fill(0) \
        .drop("enrollee_id")
    features_from_cassandra.persist()
    # объединяем микробатч из кафки и микробатч касандры
    cassandra_kafka_aggregation = features_from_cassandra
    predict = pipeline_model.transform(cassandra_kafka_aggregation)
    # predict_short = predict.select('city', 'city_development_index', 'gender', 'relevent_experience',
    #                                'enrolled_university', 'education_level', 'major_discipline', 'experience',
    #                                'comp,any_size', 'company_type', 'last_new_job', 'training_hours', 'prediction',
    #                                F.col("prediction_target").alias("target"))
    predict_short_short = features_from_kafka \
        .crossJoin(predict.select('prediction_target'))
    print("Here is what I've got after model transformation:")
    predict_short_short.show()
    # TODO: доделать сохранение предсказания обратно в Cassandra
    # обновляем исторический агрегат в касандре
    # predict_short.write \
    #     .format("org.apache.spark.sql.cassandra") \
    #     .options(table="aug_all", keyspace="koryagin") \
    #     .mode("append") \
    #     .save()
    # print("I saved the prediction and aggregation in Cassandra. Continue...")
    features_from_cassandra.unpersist()
    df.unpersist()


# связываем источник Кафки и foreachBatch функцию
stream = enrollees_df \
    .writeStream \
    .trigger(processingTime='10 seconds') \
    .foreachBatch(func=writer_logic) \
    .option("checkpointLocation", "checkpoints/sales_unknown_checkpoint")

# поехали
s = stream.start()

s.stop()
