from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("master","local[12]") \
    .getOrCreate();

RDD = spark.sparkContext.textFile("C:\\Users\Lansrod\Desktop\\truata project\pyspark_introduction\groceries.csv")\
    .map(lambda line: line.split(",")) \

print(RDD.take(10))
