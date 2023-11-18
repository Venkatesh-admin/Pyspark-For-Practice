from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode,sum
from pyspark.sql.types import *

if __name__ == '__main__':
    spark = SparkSession.builder.appName("SparkDemoApp").master("local").config("spark.sql.shuffle.partitions",10).getOrCreate()

    crime_schema = StructType([
        StructField("code", StringType()),
        StructField("borough", StringType()),
        StructField("major_category", StringType()),
        StructField("minor_category", StringType()),
        StructField("value", IntegerType()),
        StructField("year", IntegerType()),
        StructField("month", IntegerType()),
    ])

    crimeInputDF = spark.readStream.csv(path="../../dataset/streaming_data/crime_data/input",
                                        schema=crime_schema)

    # Incremental query
    # Append Mode
    resultDF = crimeInputDF.filter(col("year")==2012)
    resultDF.writeStream.format("console").option("truncate","false").outputMode("append").start().awaitTermination()

    # COMPLETE MODE
    # resultDF = crimeInputDF.groupBy(col("borough")).agg(sum("value").alias("no_convictions"))
    # resultDF.writeStream.format("console").option("truncate","false").outputMode("complete").start().awaitTermination()

    # resultDF.writeStream.format("console").option("truncate", "false").outputMode("update").start().awaitTermination()
    resultDF.show()
    # Sink
