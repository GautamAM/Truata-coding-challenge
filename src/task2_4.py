from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("master","local[12]") \
    .getOrCreate();

parquetDF = spark.read.option("mergeSchema", "true").parquet("./../sf-airbnb-clean.parquet")
parquetDF.printSchema()

min_price = float(parquetDF.describe("price").filter("summary = 'min'").select("price").first().asDict()['price'])
max_review_scores_rating = float(parquetDF.describe("review_scores_rating").filter("summary = 'max'").select("review_scores_rating").first().asDict()['review_scores_rating'])

filtredDF = parquetDF.filter(parquetDF.price == min_price).filter(parquetDF.review_scores_rating==max_review_scores_rating)
filtredDF.show()
avg_bed = float(filtredDF.describe("beds").filter("summary = 'count'").select("beds").first().asDict()['beds'])

f = open("./../out/out_2_4.txt", "w")
f.write(str(avg_bed))
f.close()