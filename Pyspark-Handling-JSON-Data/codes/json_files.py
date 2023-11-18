from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("SparkDemoApp").master("local").getOrCreate()


product = [(1,"Apple"),
          (1,"Orange"),
          (2,"Potato"),
          (2,"Beans")
          ]
category = [
   (1,"Fruits"),
   (2,"Vegetables"),
   (3,"Dry Fruits")
]


productDF = spark.createDataFrame(product).toDF("product_id","name")
categoryDF = spark.createDataFrame(category).toDF("product_id","name")


productDF.show()
categoryDF.show()


joinExpr = productDF["product_id"] ==  categoryDF["product_id"]
print(type(joinExpr))
joinType = "inner"
productDF.join(categoryDF,joinExpr,joinType).show()



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit


spark = SparkSession.builder.appName("SparkDemoApp").master("local").getOrCreate()


user_df_01 = spark.read.json(path="../dataset/user_data_json/user_01.json")
user_df_01.show()
user_df_01.printSchema()

user_df_01.select(col("firstName"),
                  concat(col("address.state"), lit(" "), col("address.postalCode")).alias("full_address")
                  ).show()


user_df_01 = spark.read.json(path="../dataset/user_data_json/user_02.json", multiLine=True)


# Scenario 03
user_df_01 = spark.read.json(path="../dataset/user_data_json/user_03.json")


# Scenario 04
user_df_01 = spark.read.json(path="../dataset/user_data_json/user_04.json", multiLine=True)
user_df_01.show()




user_df_01.printSchema()


user_df_01.select(col("firstName"),
                 concat(col("address.state"), lit(" "), col("address.postalCode")).alias("full_address")
                 ).show()


user_df_01.write.save("../dataset/output")