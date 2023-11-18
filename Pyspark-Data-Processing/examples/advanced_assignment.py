from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("testApp").master("local").getOrCreate()

#problem statement 1
#load the data
df = spark.read.text("../datasets/shakespeare.txt")
print(df.count())

#covert into array
df_01=df.withColumn("line",split(col("value"),' '))
df_01.show()
df_01.printSchema()
#explode
df_02=df_01.select('line',explode(col('line'))).withColumnRenamed('col','line_text')
df_02.show()

#clean the text
df_03=df_02.withColumn("cleaned_text",regexp_replace(col('line_text'),r'[^a-zA-Z0-9\s]',"")).drop('line')

#filter by length of word
df_04=df_03.filter(length(col("cleaned_text").cast('string'))>5)
df_04.show()

#find word count
df_05 = df_04.groupby('cleaned_text').agg(col('cleaned_text'),count("*")).select('cleaned_text')
df_05.show()