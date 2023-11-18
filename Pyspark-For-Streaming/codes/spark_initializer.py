from pyspark.sql import SparkSession


class SparkInitializer:

    @staticmethod
    def get_spark_session():
        spark = SparkSession.builder.appName("SparkDemoApp").master("local").getOrCreate()
        return spark
