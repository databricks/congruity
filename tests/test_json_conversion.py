from congruity import monkey_patch_spark
from pyspark.sql import SparkSession

def test_json_conversion():
    monkey_patch_spark()
    spark = SparkSession.builder.remote("sc://localhost").getOrCreate()

    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    df = spark.createDataFrame(data)
    assert df.count() == 3

    rows = df.toJSON().collect()
    assert len(rows) == 3