from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkDemoApp").master("local").getOrCreate()
sc=spark.sparkContext
users_rdd = sc.textFile("../datasets/sales.csv")
print(users_rdd.collect())