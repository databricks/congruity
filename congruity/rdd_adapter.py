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

from typing import Any, TYPE_CHECKING, Iterable

import pyarrow as pa
from pyarrow import RecordBatch
from pyspark import Row, RDD
import pyspark.sql.types as sqltypes
from pyspark.cloudpickle import loads, dumps

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def adapt_to_rdd(self: "DataFrame") -> "RDDAdapter":
    return RDDAdapter(self)


class RDDAdapter:
    """This class implements the RDD methods of a PySpark DataFrame, but using the
    existing DataFrame operators. This is a workaround for the fact that Spark Connect
    does not support RDD operations"""

    BIN_SCHEMA = sqltypes.StructType(
        [sqltypes.StructField("__bin_field__", sqltypes.BinaryType(), True, {"serde": "true"})]
    )

    PA_SCHEMA = pa.schema([pa.field("__bin_field__", pa.binary(), True, {"serde": "true"})])

    def __init__(self, df: "DataFrame", first_field: bool = False):
        self._df = df
        self._first_field = first_field

    def collect(self):
        data = self._df.collect()
        if self._first_field:
            assert len(self._df.schema.fields) == 1
            return [self._unnest_data(row[0]) for row in data]
        return data

    def _unnest_data(self, data: Any) -> Any:
        if isinstance(data, str):
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

    def map(self, f, preservePartitioning=None) -> "RDDAdapter":
        needs_conversion = self._first_field
        schema = RDDAdapter.PA_SCHEMA

        def mapper(iter: Iterable[RecordBatch]):
            for b in iter:
                result = []
                rows = b.to_pylist()
                for r in rows:
                    if needs_conversion:
                        val = loads(r["__bin_field__"])
                    else:
                        val = Row(**r)
                    result.append({"__bin_field__": dumps(f(val))})
                yield RecordBatch.from_pylist(result, schema=schema)

        result = self._df.mapInArrow(mapper, RDDAdapter.BIN_SCHEMA)
        assert len(result.schema.fields) == 1
        return RDDAdapter(result, True)

    map.__doc__ = RDD.map.__doc__
