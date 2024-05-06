from congruity import monkey_patch_spark
from pyspark.sql import SparkSession


def test_json_conversion(spark_session: "SparkSession"):
    monkey_patch_spark()
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    df = spark_session.createDataFrame(data)
    assert df.count() == 3

    rows = df.toJSON().collect()
    assert len(rows) == 3
    assert rows[0] == '{"_1":"Java","_2":"20000"}'
