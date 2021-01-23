import pyspark
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import IndexToString
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
from pyspark.sql import SQLContext

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("master","local[12]") \
    .getOrCreate();

customSchema = StructType([
    StructField("sepal_length", DoubleType(), True),
    StructField("sepal_width", DoubleType(), True),
    StructField("petal_length", DoubleType(), True),
    StructField("petal_width", DoubleType(), True),
    StructField("class", StringType(), True)]
)

sqlContext = SQLContext(spark.sparkContext)

df = sqlContext.read.format("csv") \
    .option("delimiter",",") \
    .schema(customSchema) \
    .load("./../iris.csv")

feature_cols = df.columns[:-1]
assembler = pyspark.ml.feature.VectorAssembler(inputCols=feature_cols, outputCol='features')
df = assembler.transform(df)

# convert text labels into indices
df = df.select(['features', 'class'])
label_indexer = pyspark.ml.feature.StringIndexer(inputCol='class', outputCol='label').fit(df)
user_labels = label_indexer.labels

df = label_indexer.transform(df)

# only select the features and label column
df = df.select(['features', 'label'])
print("Reading for machine learning")


train, test = df.randomSplit([0.70, 0.30])
test.show()

lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
model = lr.fit(train)

predictions = model.transform(test)
converter = IndexToString(inputCol="label", outputCol="originallabel")
converted = converter.transform(predictions)

converter = IndexToString(inputCol="prediction", outputCol="prediction_label",labels=user_labels)
converted = converter.transform(converted)

pred_data = spark.createDataFrame(
    [(5.1, 3.5, 1.4, 0.2),
     (6.2, 3.4, 5.4, 2.3)],
    ["sepal_length", "sepal_width", "petal_length", "petal_width"])

import csv
f = open("./../out/out_3_2.txt", "w")
features = assembler.transform(pred_data).select('features')
for row in features.take(features.count()):
    indexPredicted = int(model.predict(row['features']))
    f.write(user_labels[indexPredicted])
    f.write("\n")
f.close()
