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
import congruity  # noqa: F401
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

## Whats supported?

### RDD

| RDD                                 | API     | Comment |
|-------------------------------------|---------|---------|
| aggregate(zeroValue, seqOp, combOp) | ![open] |         |
| aggregateByKey                      | ![open] |         |
| barrier                             | ![open] |         |
| cache                               | ![open] |         |
| cartesian                           | ![open] |         |
| checkpoint                          | ![open] |         |
| cleanShuffleDependencies            | ![open] |         |
| coalesce                            | ![open] |         |
| cogroup                             | ![open] |         |
| collect                             | ![done] |         |
| collectAsMap                        | ![open] |         |
| collectWithJobGroup                 | ![open] |         |
| combineByKey                        | ![open] |         |
| count                               | ![open] |         |
| countApprox                         | ![open] |         |
| countByKey                          | ![open] |         |
| countByValue                        | ![open] |         |
| distinct                            | ![open] |         |
| filter                              | ![open] |         |
| first                               | ![done] |         |
| flatMap                             | ![open] |         |
| fold                                | ![open] |         |
| foreach                             | ![open] |         |
| foreachPartition                    | ![open] |         |
| fullOuterJoin                       | ![open] |         |
| getCheckpointFile                   | ![open] |         |
| getNumPartitions                    | ![open] |         |
| getResourceProfile                  | ![open] |         |
| getStorageLevel                     | ![open] |         |
| glom                                | ![open] |         |
| groupBy                             | ![open] |         |
| groupByKey                          | ![open] |         |
| groupWith                           | ![open] |         |
| histogram                           | ![open] |         |
| id                                  | ![open] |         |
| intersection                        | ![open] |         |
| isCheckpointed                      | ![open] |         |
| isEmpty                             | ![open] |         |
| isLocallyCheckpointed               | ![open] |         |
| join                                | ![open] |         |
| keys                                | ![open] |         |
| leftOuterJoin                       | ![open] |         |
| localCheckpoint                     | ![open] |         |
| lookup                              | ![open] |         |
| map                                 | ![done] |         |
| mapPartitions                       | ![open] |         |
| mapPartitionsWithIndex              | ![open] |         |
| mapPartitionsWithSplit              | ![open] |         |
| mapValues                           | ![open] |         |
| max                                 | ![open] |         |
| mean                                | ![open] |         |
| meanApprox                          | ![open] |         |
| min                                 | ![open] |         |
| name                                | ![open] |         |
| partitionBy                         | ![open] |         |
| persist                             | ![open] |         |
| pipe                                | ![open] |         |
| randomSplit                         | ![open] |         |
| reduce                              | ![open] |         |
| reduceByKey                         | ![open] |         |
| repartition                         | ![open] |         |
| repartitionAndSortWithinPartition   | ![open] |         |
| rightOuterJoin                      | ![open] |         |
| sample                              | ![open] |         |
| sampleByKey                         | ![open] |         |
| sampleStdev                         | ![open] |         |
| sampleVariance                      | ![open] |         |
| saveAsHadoopDataset                 | ![open] |         |
| saveAsHadoopFile                    | ![open] |         |
| saveAsNewAPIHadoopDataset           | ![open] |         |
| saveAsNewAPIHadoopFile              | ![open] |         |
| saveAsPickleFile                    | ![open] |         |
| saveAsTextFile                      | ![open] |         |
| setName                             | ![open] |         |
| sortBy                              | ![open] |         |
| sortByKey                           | ![open] |         |
| stats                               | ![open] |         |
| stdev                               | ![open] |         |
| subtract                            | ![open] |         |
| substractByKey                      | ![open] |         |
| sum                                 | ![open] |         |
| sumApprox                           | ![open] |         |
| take                                | ![done] |         |
| takeOrdered                         | ![open] |         |
| takeSample                          | ![open] |         |
| toDF                                | ![open] |         |
| toDebugString                       | ![open] |         |
| toLocalIterator                     | ![open] |         |
| top                                 | ![open] |         |
| treeAggregate                       | ![open] |         |
| treeReduce                          | ![open] |         |
| union                               | ![open] |         |
| unpersist                           | ![open] |         |
| values                              | ![open] |         |
| variance                            | ![open] |         |
| withResources                       | ![open] |         |
| zip                                 | ![open] |         |
| zipWithIndex                        | ![open] |         |
| zipWithUniqueId                     | ![open] |         |



