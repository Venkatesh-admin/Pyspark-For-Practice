from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("testApp").master("local").getOrCreate()



df=spark.read.csv("../datasets/students.csv",inferSchema=True,header=True)
# df.show()
df=df.withColumn("percentage",((df.marks1+df.marks2+df.marks3+df.marks4))/400*100).sort(col("percentage"),ascending=False)
# df.show(5)
result_df = df.withColumn("grade", when(col("percentage") <= 35, "Fail")
                                   .when((col("percentage") >= 35) & (col("percentage") < 50), "C Grade")
                                   .when((col("percentage") >= 50) & (col("percentage") < 70), "B Grade")
                                   .when((col("percentage") >= 70) & (col("percentage") < 90), "A Grade")
                                   .otherwise("S Grade"))
result_02=result_df.groupby("grade").agg(count("*").alias("count")) \
    .sort("count",ascending=False)
result_02.show()