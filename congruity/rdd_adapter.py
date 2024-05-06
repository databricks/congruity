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

from typing import Any, TYPE_CHECKING

from pyspark import Row, RDD
from pyspark.cloudpickle import loads

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class RDDAdapter:
    """This class implements the RDD methods of a PySpark DataFrame, but using the
    existing DataFrame operators. This is a workaround for the fact that Spark Connect
    does not support RDD operations"""

    def __init__(self, df: "DataFrame", first_field: bool = False):
        self._df = df
        self._first_field = first_field

    def collect(self):
        data = self._df.collect()
        if self._first_field:
            return [self._unnest_data(row[0]) for row in data]
        return data

    def _unnest_data(self, data: Any) -> Any:
        if isinstance(data, int):
            return data
        if isinstance(data, float):
            return data
        if isinstance(data, str):
            return data
        if isinstance(data, bool):
            return data
        if isinstance(data, Row):
            return data.asDict(recursive=True)
        if isinstance(data, bytearray):
            o = loads(data)
            return o
        raise NotImplementedError(f"Collecting Data type {type(data)} is not supported")

    def count(self):
        return self._df.count()

    count.__doc__ = RDD.count.__doc__

    def toDF(self, *args, **kwargs) -> "DataFrame":
        return self._df

    toDF.__doc__ = RDD.toDF.__doc__
