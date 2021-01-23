from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("master","local[12]") \
    .getOrCreate();

RDD = spark.sparkContext.textFile("C:\\Users\Lansrod\Desktop\\truata project\pyspark_introduction\groceries.csv") \
    .map(lambda line: line.split(",")) \
    .flatMap(lambda x: x)\
    .countByValue().items()

sorted = sorted(RDD,key=lambda item: item[1], reverse=True)

f = open("./../out/out_1_3.txt", "w")
for item in sorted:
    f.write(str(item)+"\n")
f.close()
