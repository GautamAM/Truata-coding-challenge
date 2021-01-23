from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("master","local[12]") \
    .getOrCreate();

parquetDF = spark.read.option("mergeSchema", "true").parquet("./../sf-airbnb-clean.parquet")
parquetDF.printSchema()