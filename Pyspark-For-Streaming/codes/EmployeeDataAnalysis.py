from pyspark.sql.functions import col, lower, split, array_contains,array_max

from spark_initializer import SparkInitializer
from data_source import DataSource

if __name__ == '__main__':
    spark = SparkInitializer.get_spark_session()
    employee_df = DataSource.get_df_for_employee()

    employee_df.show()
    employee_df.filter(col("company") == "Cisco").show()
    #
    employee_df.filter(lower(col("company")) == "cisco").show(truncate=False)
    #
    result_df =(employee_df.withColumn("skills_arr",split(col("skills"),","))
                .drop("skills")
                .withColumnRenamed("skills_arr","skills"))
    result_df.printSchema()
    result_df.show()

    #
    df_01 = result_df.withColumn("primary_skill", col("skills").getItem(0) )
    df_01.select("name","exp", "company","skills", "primary_skill").show()
    #
    print("Fetch all the records who has techical skill  PySpark")
    result_df.withColumn("required_skills", array_contains(col("skills"),"PySpark"))\
     .filter(col("required_skills")==True) \
     .show(truncate=False)
    #
    result_df = (employee_df.withColumn("actual_expected_salary_arr", split(col("actual_expected_salary"), ",").cast('array<integer>'))
                 .drop("actual_expected_salary")
                 .withColumnRenamed("actual_expected_salary_arr", "actual_expected_salary"))
    df_02 = result_df.withColumn("max_salary", array_max(col("actual_expected_salary"),))
    df_02.show()


