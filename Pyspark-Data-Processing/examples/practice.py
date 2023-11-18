from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("testApp").master("local").getOrCreate()
sc=spark.sparkContext

def rdd_collection():
    users = [(1,"venkatesh"),(2,"mahesh")]
    users_rdd=sc.parallelize(users)

    return users_rdd.collect()
def external_rdd():
    users = sc.textFile("../datasets/users_01.dat")
    users_rdd=sc.parallelize(users)
    return users.collect()

def dataframe_from_collection():
    users = [(1, "venkatesh"), (2, "mahesh")]
    users=spark.createDataFrame(users).toDF("id","name")
    return users.show()

def dataframe_from_csv():
    users = spark.read.csv("../datasets/users_01.dat",inferSchema=True,sep="|",header=True)
    # return users.show()
    # return users.select(col('id'),col('city')).show()
    # return users.filter(col("gender")=='M').select(col("gender"),col("id"),col("state"),col("asset")).show()

    # result_df = users.withColumn("category", when(col("asset") < 500000, "Worker")
    #                                .when((col("asset") >= 500000) & (col("asset") < 1000000), "Middle")
    #                                .when(col("asset") >= 1000000, "Upper")
    #                                .otherwise("Wrong value")).show()
    # return users.groupby("gender").min("asset").show()
    return users.groupby("gender").agg(min("asset").alias("min_asset"),max("asset").alias("max_asset"),count("asset").alias("asset_count")).show()
print(dataframe_from_csv())




