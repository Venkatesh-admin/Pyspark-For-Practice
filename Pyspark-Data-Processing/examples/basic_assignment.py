from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("testApp").master("local").getOrCreate()
sc=spark.sparkContext

data = [(1,"venkatesh"),(2,"mahesh")]
rdd=sc.parallelize(data)
print(rdd.collect())

# Define the schema for the DataFrame
schema = ["id", "name"]

# Convert the RDD to a DataFrame
df = spark.createDataFrame(rdd, schema=schema)

# Show the DataFrame
df.show()

#printschema
df.printSchema()

print("columns*****************",df.columns)

df.select("name").show()