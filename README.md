# congruity

Spark JVM & Spark Connect compatibility library.

Here is code that works on Spark JVM:

```python
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
spark.sparkContext.parallelize(data).toDF()
```

This code doesn't work with Spark Connect.  The congruity library rearranges the code under the hood, so the old syntax works on Spark Connect clusters as well:

```python
from congruity import *

data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
spark.sparkContext.parallelize(data).toDF()
```
