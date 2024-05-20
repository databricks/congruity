# congruity

[![GitHub Actions Build](https://github.com/databricks/congruity/actions/workflows/main.yml/badge.svg)](https://github.com/databricks/congruity/actions/workflows/main.yml)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/spark-congruity?period=month&units=international_system&left_color=black&right_color=orange&left_text=PyPI%20downloads)](https://pypi.org/project/spark-congruity/)

In many ways, the migration from using classic Spark applications using the full
power and flexibility to be using only the Spark Connect compatible DataFrame API
can be challenging.

The goal of this library is to provide a compatibility layer that makes it easier to
adopt Spark Connect. The library is designed to be simply imported in your application
and will then monkey-patch the existing API to provide the legacy functionality.

## Non-Goals

This library is not intended to be a long-term solution. The goal is to provide a
compatibility layer that becomes obsolete over time. In addition, we do not aim to
provide compatibility for all methods and features but only a select subset. Lastly,
we do not aim to achieve the same performance as using some of the native RDD APIs.

## Usage

Spark JVM & Spark Connect compatibility library.

```shell
pip install spark-congruity
```

```python
import congruity
```

### Example

Here is code that works on Spark JVM:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost").getOrCreate()
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
spark.sparkContext.parallelize(data).toDF()
```

This code doesn't work with Spark Connect. The congruity library rearranges the code under the hood, so the old syntax
works on Spark Connect clusters as well:

```python
import congruity  # noqa: F401
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost").getOrCreate()
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
spark.sparkContext.parallelize(data).toDF()
```

## Contributing

We very much welcome contributions to this project. The easiest way to start is to pick any of
the below RDD or SparkContext methods and implement the compatibility layer. Once you have done
that open a pull request and we will review it.

## What's supported?

### RDD

| RDD                               | API                | Comment                                                           |
|-----------------------------------|--------------------|-------------------------------------------------------------------|
| aggregate                         | :white_check_mark: |                                                                   |
| aggregateByKey                    | :x:                |                                                                   |
| barrier                           | :x:                |                                                                   |
| cache                             | :x:                |                                                                   |
| cartesian                         | :x:                |                                                                   |
| checkpoint                        | :x:                |                                                                   |
| cleanShuffleDependencies          | :x:                |                                                                   |
| coalesce                          | :x:                |                                                                   |
| cogroup                           | :x:                |                                                                   |
| collect                           | :white_check_mark: |                                                                   |
| collectAsMap                      | :x:                |                                                                   |
| collectWithJobGroup               | :x:                |                                                                   |
| combineByKey                      | :x:                |                                                                   |
| count                             | :white_check_mark: |                                                                   |
| countApprox                       | :x:                |                                                                   |
| countByKey                        | :x:                |                                                                   |
| countByValue                      | :x:                |                                                                   |
| distinct                          | :x:                |                                                                   |
| filter                            | :white_check_mark: |                                                                   |
| first                             | :white_check_mark: |                                                                   |
| flatMap                           | :x:                |                                                                   |
| fold                              | :white_check_mark: | First version                                                     |
| foreach                           | :x:                |                                                                   |
| foreachPartition                  | :x:                |                                                                   |
| fullOuterJoin                     | :x:                |                                                                   |
| getCheckpointFile                 | :x:                |                                                                   |
| getNumPartitions                  | :x:                |                                                                   |
| getResourceProfile                | :x:                |                                                                   |
| getStorageLevel                   | :x:                |                                                                   |
| glom                              | :white_check_mark: |                                                                   |
| groupBy                           | :x:                |                                                                   |
| groupByKey                        | :x:                |                                                                   |
| groupWith                         | :x:                |                                                                   |
| histogram                         | :white_check_mark: |                                                                   |
| id                                | :x:                |                                                                   |
| intersection                      | :x:                |                                                                   |
| isCheckpointed                    | :x:                |                                                                   |
| isEmpty                           | :x:                |                                                                   |
| isLocallyCheckpointed             | :x:                |                                                                   |
| join                              | :x:                |                                                                   |
| keyBy                             | :white_check_mark: |                                                                   |
| keys                              | :white_check_mark: |                                                                   |
| leftOuterJoin                     | :x:                |                                                                   |
| localCheckpoint                   | :x:                |                                                                   |
| lookup                            | :x:                |                                                                   |
| map                               | :white_check_mark: |                                                                   |
| mapPartitions                     | :white_check_mark: | First version, based on mapInArrow.                               |
| mapPartitionsWithIndex            | :x:                |                                                                   |
| mapPartitionsWithSplit            | :x:                |                                                                   |
| mapValues                         | :x:                |                                                                   |
| max                               | :white_check_mark: |                                                                   |
| mean                              | :white_check_mark: |                                                                   |
| meanApprox                        | :x:                |                                                                   |
| min                               | :white_check_mark: |                                                                   |
| name                              | :x:                |                                                                   |
| partitionBy                       | :x:                |                                                                   |
| persist                           | :x:                |                                                                   |
| pipe                              | :x:                |                                                                   |
| randomSplit                       | :x:                |                                                                   |
| reduce                            | :white_check_mark: |                                                                   |
| reduceByKey                       | :x:                |                                                                   |
| repartition                       | :x:                |                                                                   |
| repartitionAndSortWithinPartition | :x:                |                                                                   |
| rightOuterJoin                    | :x:                |                                                                   |
| sample                            | :x:                |                                                                   |
| sampleByKey                       | :x:                |                                                                   |
| sampleStdev                       | :white_check_mark: |                                                                   |
| sampleVariance                    | :white_check_mark: |                                                                   |
| saveAsHadoopDataset               | :x:                |                                                                   |
| saveAsHadoopFile                  | :x:                |                                                                   |
| saveAsNewAPIHadoopDataset         | :x:                |                                                                   |
| saveAsNewAPIHadoopFile            | :x:                |                                                                   |
| saveAsPickleFile                  | :x:                |                                                                   |
| saveAsTextFile                    | :x:                |                                                                   |
| setName                           | :x:                |                                                                   |
| sortBy                            | :x:                |                                                                   |
| sortByKey                         | :x:                |                                                                   |
| stats                             | :white_check_mark: |                                                                   |
| stdev                             | :white_check_mark: |                                                                   |
| subtract                          | :x:                |                                                                   |
| substractByKey                    | :x:                |                                                                   |
| sum                               | :white_check_mark: | First version.                                                    |
| sumApprox                         | :x:                |                                                                   |
| take                              | :white_check_mark: | Ordering might not be guaranteed in the same way as it is in RDD. |
| takeOrdered                       | :x:                |                                                                   |
| takeSample                        | :x:                |                                                                   |
| toDF                              | :white_check_mark: |                                                                   |
| toDebugString                     | :x:                |                                                                   |
| toLocalIterator                   | :x:                |                                                                   |
| top                               | :x:                |                                                                   |
| treeAggregate                     | :x:                |                                                                   |
| treeReduce                        | :x:                |                                                                   |
| union                             | :x:                |                                                                   |
| unpersist                         | :x:                |                                                                   |
| values                            | :white_check_mark: |                                                                   |
| variance                          | :white_check_mark: |                                                                   |
| withResources                     | :x:                |                                                                   |
| zip                               | :x:                |                                                                   |
| zipWithIndex                      | :x:                |                                                                   |
| zipWithUniqueId                   | :x:                |                                                                   |

### SparkContext

| RDD         | API                | Comment                         |
|-------------|--------------------|---------------------------------|
| parallelize | :white_check_mark: | Does not support numSlices yet. |

