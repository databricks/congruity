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
import operator
from collections.abc import Iterator

import pyspark.sql.connect.session
import pytest
from pyspark import Row

from congruity import monkey_patch_spark
from pyspark.sql import SparkSession, DataFrame
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


def test_take(spark_session: "SparkSession"):
    monkey_patch_spark()
    df = spark_session.range(10)

    vals = df.rdd.map(lambda x: (x[0], 99)).take(3)
    assert vals == [(0, 99), (1, 99), (2, 99)]


def test_map_partitions(spark_session: "SparkSession"):
    monkey_patch_spark()
    df = spark_session.range(10)

    def f(it):
        for x in it:
            yield x[0] * 2

    vals = df.rdd.mapPartitions(f).collect()
    assert vals == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]


def test_pipelineing_does_only_have_one_job(spark_session: "SparkSession"):
    if isinstance(spark_session, pyspark.sql.connect.session.SparkSession):
        monkey_patch_spark()
        df = spark_session.range(10).repartition(1)

        # Two chained map partitions
        first = df.rdd.mapPartitions(lambda i: [sum(1 for _ in i)])
        second = first.mapPartitions(lambda x: [sum(x)])

        import pyspark.sql.connect.plan as plan

        assert isinstance(second._prev_source._plan, plan.Repartition)
        res = second.collect()
        assert res == [10]


def test_count(spark_session: "SparkSession"):
    monkey_patch_spark()
    df = spark_session.range(10)

    vals = df.rdd.count()
    assert vals == 10


def test_rdd_fold(spark_session: "SparkSession"):
    monkey_patch_spark()
    df = spark_session.range(10)

    vals = df.rdd.map(lambda x: x[0]).fold(0, lambda x, y: x + y)
    assert vals == 45

    df = (
        spark_session.range(10000)
        .repartition(1)
        .rdd.map(lambda x: x[0])
        .fold(0, lambda x, y: x + y)
    )
    assert df == 49995000


def test_rdd_sum(spark_session: "SparkSession"):
    monkey_patch_spark()
    df = spark_session.range(10)

    vals = df.rdd.map(lambda x: x[0]).sum()
    assert vals == 45


def test_rdd_keys(spark_session: "SparkSession"):
    monkey_patch_spark()
    rdd = spark_session.sparkContext.parallelize([(1, 2), (3, 4)]).keys()
    assert rdd.collect() == [1, 3]


def test_rdd_values(spark_session: "SparkSession"):
    monkey_patch_spark()
    rdd = spark_session.sparkContext.parallelize([(1, 2), (3, 4)]).values()
    assert rdd.collect() == [2, 4]


def test_rdd_glom(spark_session: "SparkSession"):
    monkey_patch_spark()
    rdd = spark_session.sparkContext.parallelize(range(10), 2).glom().collect()
    assert len(rdd) == 2


def test_rdd_key_by(spark_session: "SparkSession"):
    monkey_patch_spark()
    rdd = spark_session.sparkContext.parallelize(range(10)).keyBy(lambda x: x * 2)
    assert rdd.collect() == [
        (0, 0),
        (2, 1),
        (4, 2),
        (6, 3),
        (8, 4),
        (10, 5),
        (12, 6),
        (14, 7),
        (16, 8),
        (18, 9),
    ]


def test_rdd_stats(spark_session: "SparkSession"):
    monkey_patch_spark()
    rdd = spark_session.sparkContext.parallelize(range(10))
    stats = rdd.stats()
    assert stats.count() == 10
    assert stats.mean() == 4.5
    assert stats.sum() == 45
    assert stats.min() == 0
    assert stats.max() == 9
    assert stats.stdev() == 2.8722813232690143
    assert stats.variance() == 8.25
    assert stats.sampleStdev() == 3.0276503540974917
    assert stats.sampleVariance() == 9.166666666666666


def test_rdd_stddev(spark_session: "SparkSession"):
    monkey_patch_spark()
    rdd = spark_session.sparkContext.parallelize(range(10))
    assert rdd.stdev() == 2.8722813232690143


def test_rdd_sample_stddev(spark_session: "SparkSession"):
    monkey_patch_spark()
    rdd = spark_session.sparkContext.parallelize(range(10))
    assert rdd.sampleStdev() == 3.0276503540974917


def test_rdd_sample_variance(spark_session: "SparkSession"):
    monkey_patch_spark()
    rdd = spark_session.sparkContext.parallelize(range(10))
    assert rdd.sampleVariance() == 9.166666666666666


def test_rdd_variance(spark_session: "SparkSession"):
    monkey_patch_spark()
    rdd = spark_session.sparkContext.parallelize(range(10))
    assert rdd.variance() == 8.25


def test_rdd_aggregate(spark_session: "SparkSession"):
    monkey_patch_spark()
    rdd = spark_session.sparkContext.parallelize(range(10))
    assert rdd.aggregate(0, lambda x, y: x + y, lambda x, y: x + y) == 45

    seqOp = lambda x, y: (x[0] + y, x[1] + 1)
    combOp = lambda x, y: (x[0] + y[0], x[1] + y[1])
    res = spark_session.sparkContext.parallelize([1, 2, 3, 4]).aggregate((0, 0), seqOp, combOp)
    assert res == (10, 4)

    # TODO empty
    # res = spark_session.sparkContext.parallelize([]).aggregate((0, 0), seqOp, combOp)
    # assert res == (0, 0)


def test_rdd_min(spark_session: "SparkSession"):
    monkey_patch_spark()
    df = spark_session.range(10).repartition(1)
    assert df.rdd.map(lambda x: x[0]).min() == 0


def test_rdd_max(spark_session: "SparkSession"):
    monkey_patch_spark()
    df = spark_session.range(10).repartition(1)
    assert df.rdd.map(lambda x: x[0]).max() == 9


def test_rdd_histogram(spark_session: "SparkSession"):
    monkey_patch_spark()
    rdd = spark_session.sparkContext.parallelize(range(10))
    assert rdd.histogram(3) == ([0, 3, 6, 9], [3, 3, 4])


def test_rdd_filter(spark_session: "SparkSession"):
    monkey_patch_spark()
    rdd = spark_session.sparkContext.parallelize(range(10))
    assert rdd.filter(lambda x: x % 2 == 0).collect() == [0, 2, 4, 6, 8]


def test_rdd_mean(spark_session: "SparkSession"):
    monkey_patch_spark()
    rdd = spark_session.sparkContext.parallelize(range(10))
    assert rdd.mean() == 4.5


def test_rdd_variance(spark_session: "SparkSession"):
    monkey_patch_spark()
    rdd = spark_session.sparkContext.parallelize(range(10))
    assert rdd.variance() == 8.25
