import pytest
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost").getOrCreate()

class FakeSparkContext():
    def parallelize(self, data):
        return FakeParallelizedCollection(data)

class FakeParallelizedCollection():
    def __init__(self, data):
        self.data = data
    
    def toDF():


def test_spark_context_monkey():
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    spark.sparkContext.parallelize(data).toDF()
    