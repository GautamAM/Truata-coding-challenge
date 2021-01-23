from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("master","local[12]") \
    .getOrCreate();

parquetDF = spark.read.option("mergeSchema", "true").parquet("./../sf-airbnb-clean.parquet")
parquetDF.printSchema()
filtredDF = parquetDF.filter(parquetDF.price>5000).filter(parquetDF.review_scores_accuracy==10)
print(filtredDF.count())
avg_bathrooms = float(parquetDF.describe("bathrooms").filter("summary = 'mean'").select("bathrooms").first().asDict()['bathrooms'])
avg_bedrooms = float(parquetDF.describe("bedrooms").filter("summary = 'mean'").select("bedrooms").first().asDict()['bedrooms'])

print(avg_bathrooms)
print(avg_bedrooms)

import csv
with open('./../out/out_2_3.txt', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["avg_bathrooms", "avg_bedrooms"])
    writer.writerow([avg_bathrooms, avg_bedrooms])