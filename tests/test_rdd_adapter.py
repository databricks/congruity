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
from pyspark import Row

from congruity import monkey_patch_spark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructType, StructField

from pyspark.cloudpickle.cloudpickle import register_pickle_by_value


class FakeObject:
    def __init__(self, data):
        self.data = data

    def value(self):
        return self.data


import tests

register_pickle_by_value(tests)


def test_basic_mapper(spark_session: "SparkSession"):
    monkey_patch_spark()
    df = spark_session.range(10)

    vals = df.rdd.map(lambda x: x[0]).collect()
    assert vals == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    vals = df.rdd.map(lambda x: FakeObject(x[0] * 3)).map(lambda x: x.value()).collect()
    assert vals == [0, 3, 6, 9, 12, 15, 18, 21, 24, 27]


def test_first(spark_session: "SparkSession"):
    monkey_patch_spark()
    df = spark_session.range(10)

    vals = df.rdd.first()
    assert vals == Row(id=0)

    vals = df.rdd.map(lambda x: x[0]).first()
    assert vals == 0


def test_to_df(spark_session: "SparkSession"):
    monkey_patch_spark()
    df = spark_session.range(10)

    vals = (
        df.rdd.map(lambda x: (x[0], 99))
        .toDF(
            schema=StructType(
                [StructField("age", IntegerType()), StructField("pulse", IntegerType())]
            )
        )
        .collect()
    )

    row = vals[0]
    assert row.age == 0
    assert row.pulse == 99


def test_to_df_no_schema(spark_session: "SparkSession"):
    monkey_patch_spark()
    df = spark_session.range(10)

    vals = df.rdd.map(lambda x: (x[0], "99")).toDF().collect()

    row = vals[0]
    assert row._1 == 0
    assert row._2 == "99"
