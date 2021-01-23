from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("master","local") \
    .getOrCreate();

RDD = spark.sparkContext.textFile("C:\\Users\Lansrod\Desktop\\truata project\pyspark_introduction\groceries.csv") \
    .map(lambda line: line.split(","))\
    .flatMap(lambda x: x)
print(RDD.collect())
RDD.distinct().repartition(1).saveAsTextFile("./../out/out_1_2a.txt")

f = open("./../out/out_1_2b.txt", "w")
f.write(RDD.count().__str__())
f.close()


