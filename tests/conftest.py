import pytest


@pytest.fixture(scope="module", params=["classic", "connect"])
def spark_session(request):
    if request.param == "classic":
        import os

        if "SPARK_CONNECT_MODE_ENABLED" in os.environ:
            del os.environ["SPARK_CONNECT_MODE_ENABLED"]
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local").appName("test").getOrCreate()
        yield spark
        spark.sparkContext.stop()
    else:
        from pyspark.sql.connect.session import SparkSession as RSS

        spark = RSS.builder.remote("sc://localhost").create()
        yield spark
        spark.stop()
