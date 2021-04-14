"""
export SPARK_KAFKA_VERSION=0.10
/spark2.4/bin/pyspark \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 \
    --driver-memory 512m \
    --driver-cores 1 \
    --master local[1]
"""

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, FloatType, DoubleType
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler, StringIndexer, IndexToString

spark = SparkSession.builder.appName("gogin_spark").getOrCreate()

aug_schema = StructType([
    StructField('enrollee_id', IntegerType(), nullable=False),
    StructField('city', StringType(), nullable=True),
    StructField('city_development_index', FloatType(), nullable=True),
    StructField('gender', StringType(), nullable=True),
    StructField('relevent_experience', StringType(), nullable=True),
    StructField('enrolled_university', StringType(), nullable=True),
    StructField('education_level', StringType(), nullable=True),
    StructField('major_discipline', StringType(), nullable=True),
    StructField('experience', StringType(), nullable=True),
    StructField('comp,any_size', StringType(), nullable=True),
    StructField('company_type', StringType(), nullable=True),
    StructField('last_new_job', StringType(), nullable=True),
    StructField('training_hours', IntegerType(), nullable=True),
    StructField('target', DoubleType(), nullable=False)
])

enrollee_known = spark.read \
    .options(delimiter=',', inferschema=True, header=True) \
    .csv(path="input_csv_for_stream/aug_train.csv")

# enrollee_known.show(n=10, truncate=True)

# в общем - все анализируемые колонки заносим в колонку-вектор features
categoricalColumns = ['city', 'gender', 'relevent_experience', 'enrolled_university', 'education_level',
                      'major_discipline', 'experience', 'company_type', 'last_new_job']
numericCols = ['city_development_index', 'training_hours']

stages = []
for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer(
        inputCol=categoricalCol, outputCol=categoricalCol + 'Index').setHandleInvalid("keep")
    encoder = OneHotEncoderEstimator(
        inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"]).setHandleInvalid("keep")
    stages += [stringIndexer, encoder]

label_stringIdx = StringIndexer(inputCol='target', outputCol='label').setHandleInvalid("keep")
stages += [label_stringIdx]

assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features").setHandleInvalid("keep")
stages += [assembler]

lr = LogisticRegression(featuresCol='features', labelCol='label', maxIter=10)
stages += [lr]

label_stringIdx_fit = label_stringIdx.fit(dataset=enrollee_known)
indexToStringEstimator = IndexToString() \
    .setInputCol("prediction") \
    .setOutputCol("prediction_target") \
    .setLabels(label_stringIdx_fit.labels)

stages += [indexToStringEstimator]

pipeline = Pipeline().setStages(stages)
pipelineModel = pipeline.fit(dataset=enrollee_known)

# для наглядности можно посчитать процент полной сходимости
# pipelineModel.transform(enrollee_known).select("prediction_target", "target").show(n=30)

# сохраняем модель на HDFS
pipelineModel.write().overwrite().save("ml_models/my_LR_model_enrollees")
