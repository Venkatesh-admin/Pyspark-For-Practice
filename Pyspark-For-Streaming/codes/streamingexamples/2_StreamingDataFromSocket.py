from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode

if __name__ == '__main__':

    spark = SparkSession.builder.appName("SparkDemoApp").master("local").config("spark.sql.shuffle.partitions",10).getOrCreate()

    # Input Table
    inputDF = spark.readStream.format("socket").option("host","localhost").option("port","9901").load()

    # Incremental Query
    resultDF = inputDF.select(explode(split(col("value")," ")).alias("words")).groupBy("words").count()

    # Sink
    resultDF.writeStream.format("console").outputMode("complete").start().awaitTermination()
