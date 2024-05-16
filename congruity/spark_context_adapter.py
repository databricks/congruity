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

from typing import Any, Optional
from pyspark.cloudpickle import dumps
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.session import SparkSession

from congruity.rdd_adapter import RDDAdapter


class SparkContextAdapter:
    _spark: SparkSession

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def parallelize(self, data: Any, slices: Optional[int] = None) -> "RDDAdapter":
        # Create the binary DF from the data
        serialized = map(lambda x: dumps(x), data)
        return RDDAdapter(self._spark.createDataFrame(serialized, RDDAdapter.BIN_SCHEMA), first_field=True)


def adapt_to_spark_context(self: DataFrame) -> SparkContextAdapter:
    return SparkContextAdapter(self)
