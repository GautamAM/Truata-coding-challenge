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

df = sqlContext.read.format("csv")\
    .option("delimiter",",") \
    .schema(customSchema)\
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

df.show(10)


train, test = df.randomSplit([0.70, 0.30])
test.show()

lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
model = lr.fit(train)

predictions = model.transform(test)
converter = IndexToString(inputCol="label", outputCol="originallabel")
converted = converter.transform(predictions)

converter = IndexToString(inputCol="prediction", outputCol="prediction_label",labels=user_labels)
converted = converter.transform(converted)
converted.show(5)

customSchema = StructType([
    StructField("sepal_length", DoubleType(), True),
    StructField("sepal_width", DoubleType(), True),
    StructField("petal_length", DoubleType(), True),
    StructField("petal_width", DoubleType(), True)])
myrdd = spark.sparkContext.parallelize([[5.1, 3.5, 1.4, 0.2]])
df = sqlContext.createDataFrame(myrdd,  customSchema)
features = assembler.transform(df).select('features')
indexPredicted = int(model.predict(features.first()['features']))
print(user_labels[indexPredicted])


myrdd = spark.sparkContext.parallelize([[6.2, 3.4, 5.4, 2.3]])
df = sqlContext.createDataFrame(myrdd,  customSchema)
features = assembler.transform(df).select('features')
indexPredicted = int(model.predict(features.first()['features']))
print(user_labels[indexPredicted])
