# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from congruity import monkey_patch_spark
from pyspark.sql import SparkSession


def test_spark_context(spark_session: "SparkSession"):
    monkey_patch_spark()
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    result = spark_session.sparkContext.parallelize(data).toDF()
    assert result.count() == 3


def test_spark_context_parallelize(spark_session: "SparkSession"):
    monkey_patch_spark()
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    result = (
        spark_session.sparkContext.parallelize(data)
        .map(lambda x: x[1])
        .map(lambda x: int(x))
        .collect()
    )
    assert result == [20000, 100000, 3000]

    val = spark_session.sparkContext.parallelize(list(range(0, 5)))
    assert val.count() == 5
    assert val.collect() == [0, 1, 2, 3, 4]
