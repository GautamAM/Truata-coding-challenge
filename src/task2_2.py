from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("master","local[12]") \
    .getOrCreate();

parquetDF = spark.read.option("mergeSchema", "true").parquet("./../sf-airbnb-clean.parquet")
parquetDF.printSchema()
min_price = float(parquetDF.describe("price").filter("summary = 'min'").select("price").first().asDict()['price'])
max_price = float(parquetDF.describe("price").filter("summary = 'max'").select("price").first().asDict()['price'])
row_count = parquetDF.count()

parquetDF.describe("price").show()

import csv
with open('./../out/out_2_2.txt', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["min_price", "max_price", "row_count"])
    writer.writerow([min_price, max_price, row_count])