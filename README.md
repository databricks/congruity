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
pip install congruity
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

This code doesn't work with Spark Connect.  The congruity library rearranges the code under the hood, so the old syntax works on Spark Connect clusters as well:

```python
import congruity # noqa: F401
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost").getOrCreate()
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
spark.sparkContext.parallelize(data).toDF()
```
