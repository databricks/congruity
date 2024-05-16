# congruity

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
| aggregate                         | :x:                |                                                                   |
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
| count                             | :x:                |                                                                   |
| countApprox                       | :x:                |                                                                   |
| countByKey                        | :x:                |                                                                   |
| countByValue                      | :x:                |                                                                   |
| distinct                          | :x:                |                                                                   |
| filter                            | :x:                |                                                                   |
| first                             | :white_check_mark: |                                                                   |
| flatMap                           | :x:                |                                                                   |
| fold                              | :x:                |                                                                   |
| foreach                           | :x:                |                                                                   |
| foreachPartition                  | :x:                |                                                                   |
| fullOuterJoin                     | :x:                |                                                                   |
| getCheckpointFile                 | :x:                |                                                                   |
| getNumPartitions                  | :x:                |                                                                   |
| getResourceProfile                | :x:                |                                                                   |
| getStorageLevel                   | :x:                |                                                                   |
| glom                              | :x:                |                                                                   |
| groupBy                           | :x:                |                                                                   |
| groupByKey                        | :x:                |                                                                   |
| groupWith                         | :x:                |                                                                   |
| histogram                         | :x:                |                                                                   |
| id                                | :x:                |                                                                   |
| intersection                      | :x:                |                                                                   |
| isCheckpointed                    | :x:                |                                                                   |
| isEmpty                           | :x:                |                                                                   |
| isLocallyCheckpointed             | :x:                |                                                                   |
| join                              | :x:                |                                                                   |
| keys                              | :x:                |                                                                   |
| leftOuterJoin                     | :x:                |                                                                   |
| localCheckpoint                   | :x:                |                                                                   |
| lookup                            | :x:                |                                                                   |
| map                               | :white_check_mark: |                                                                   |
| mapPartitions                     | :x:                |                                                                   |
| mapPartitionsWithIndex            | :x:                |                                                                   |
| mapPartitionsWithSplit            | :x:                |                                                                   |
| mapValues                         | :x:                |                                                                   |
| max                               | :x:                |                                                                   |
| mean                              | :x:                |                                                                   |
| meanApprox                        | :x:                |                                                                   |
| min                               | :x:                |                                                                   |
| name                              | :x:                |                                                                   |
| partitionBy                       | :x:                |                                                                   |
| persist                           | :x:                |                                                                   |
| pipe                              | :x:                |                                                                   |
| randomSplit                       | :x:                |                                                                   |
| reduce                            | :x:                |                                                                   |
| reduceByKey                       | :x:                |                                                                   |
| repartition                       | :x:                |                                                                   |
| repartitionAndSortWithinPartition | :x:                |                                                                   |
| rightOuterJoin                    | :x:                |                                                                   |
| sample                            | :x:                |                                                                   |
| sampleByKey                       | :x:                |                                                                   |
| sampleStdev                       | :x:                |                                                                   |
| sampleVariance                    | :x:                |                                                                   |
| saveAsHadoopDataset               | :x:                |                                                                   |
| saveAsHadoopFile                  | :x:                |                                                                   |
| saveAsNewAPIHadoopDataset         | :x:                |                                                                   |
| saveAsNewAPIHadoopFile            | :x:                |                                                                   |
| saveAsPickleFile                  | :x:                |                                                                   |
| saveAsTextFile                    | :x:                |                                                                   |
| setName                           | :x:                |                                                                   |
| sortBy                            | :x:                |                                                                   |
| sortByKey                         | :x:                |                                                                   |
| stats                             | :x:                |                                                                   |
| stdev                             | :x:                |                                                                   |
| subtract                          | :x:                |                                                                   |
| substractByKey                    | :x:                |                                                                   |
| sum                               | :x:                |                                                                   |
| sumApprox                         | :x:                |                                                                   |
| take                              | :white_check_mark: | Ordering might not be guaranteed in the same way as it is in RDD. |
| takeOrdered                       | :x:                |                                                                   |
| takeSample                        | :x:                |                                                                   |
| toDF                              | :x:                |                                                                   |
| toDebugString                     | :x:                |                                                                   |
| toLocalIterator                   | :x:                |                                                                   |
| top                               | :x:                |                                                                   |
| treeAggregate                     | :x:                |                                                                   |
| treeReduce                        | :x:                |                                                                   |
| union                             | :x:                |                                                                   |
| unpersist                         | :x:                |                                                                   |
| values                            | :x:                |                                                                   |
| variance                          | :x:                |                                                                   |
| withResources                     | :x:                |                                                                   |
| zip                               | :x:                |                                                                   |
| zipWithIndex                      | :x:                |                                                                   |
| zipWithUniqueId                   | :x:                |                                                                   |

### SparkContext

| RDD         | API                | Comment                         |
|-------------|--------------------|---------------------------------|
| parallelize | :white_check_mark: | Does not support numSlices yet. |

