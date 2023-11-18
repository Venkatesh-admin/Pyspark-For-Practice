from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.appName("SparkDemoApp").master("local").getOrCreate()
    inputDF = spark.readStream.format("socket").option("host","localhost").option("port","9901").load()
    inputDF.writeStream.format("console").outputMode("append").start().awaitTermination()
