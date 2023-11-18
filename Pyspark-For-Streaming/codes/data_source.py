from pyspark.sql.types import *

from spark_initializer import SparkInitializer


class DataSource:

    @staticmethod
    def get_df_for_employee():
        custom_schema = StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("exp", IntegerType()),
            StructField("gen", StringType()),
            StructField("dob", TimestampType()),
            StructField("company", StringType()),
            StructField("designation", StringType()),
            StructField("doj", StringType()),
            StructField("skills", StringType()),
            StructField("actual_expected_salary", StringType())
        ])
        spark = SparkInitializer.get_spark_session()
        employee_df = spark.read.csv(path="../dataset/employee.csv",
                                     sep="|",
                                     header=True,
                                     schema=custom_schema,
                                     quote="'")
        return employee_df

    @staticmethod
    def get_df_from_sms():
        pass

    @staticmethod
    def get_df_5g():
        pass


    def call_me(self):
        pass
