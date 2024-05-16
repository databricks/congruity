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

from typing import Any, TYPE_CHECKING, Iterable, Optional, Union, no_type_check, Tuple, List, Sized

import pyarrow as pa
from pyarrow import RecordBatch
from pyspark import Row, RDD
import pyspark.sql.types as sqltypes
from pyspark.cloudpickle import loads, dumps
from pyspark.errors import PySparkValueError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def adapt_to_rdd(self: "DataFrame") -> "RDDAdapter":
    return RDDAdapter(self)


def _convert_spark_type_to_pyarrow_type(spark_type: sqltypes.DataType) -> pa.DataType:
    if isinstance(spark_type, sqltypes.BinaryType):
        return pa.binary()
    if isinstance(spark_type, sqltypes.BooleanType):
        return pa.bool_()
    if isinstance(spark_type, sqltypes.ByteType):
        return pa.int8()
    if isinstance(spark_type, sqltypes.ShortType):
        return pa.int16()
    if isinstance(spark_type, sqltypes.IntegerType):
        return pa.int32()
    if isinstance(spark_type, sqltypes.LongType):
        return pa.int64()
    if isinstance(spark_type, sqltypes.FloatType):
        return pa.float32()
    if isinstance(spark_type, sqltypes.DoubleType):
        return pa.float64()
    if isinstance(spark_type, sqltypes.StringType):
        return pa.string()
    if isinstance(spark_type, sqltypes.DateType):
        return pa.date32()
    if isinstance(spark_type, sqltypes.TimestampType):
        return pa.timestamp("ns")
    if isinstance(spark_type, sqltypes.ArrayType):
        return pa.list_(_convert_spark_type_to_pyarrow_type(spark_type.elementType))
    if isinstance(spark_type, sqltypes.MapType):
        return pa.map_(
            _convert_spark_type_to_pyarrow_type(spark_type.keyType),
            _convert_spark_type_to_pyarrow_type(spark_type.valueType),
        )
    if isinstance(spark_type, sqltypes.StructType):
        return _convert_spark_schema_to_pyarrow_schema(spark_type)
    raise NotImplementedError(f"Conversion of type {spark_type} is not supported")


def _convert_spark_schema_to_pyarrow_schema(schema: sqltypes.StructType) -> pa.Schema:
    return pa.schema(
        [
            pa.field(field.name, _convert_spark_type_to_pyarrow_type(field.dataType), True)
            for field in schema.fields
        ]
    )


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
        if any(map(lambda x: isinstance(data, x), (str, Row, int, float, bool, type(None)))):
            return data
        if isinstance(data, bytearray):
            o = loads(data)
            return o
        raise NotImplementedError(f"Collecting Data type {type(data)} is not supported")

    def count(self):
        return self._df.count()

    count.__doc__ = RDD.count.__doc__

    def toDF(
        self,
        schema: Optional[Union[sqltypes.AtomicType, sqltypes.StructType, str]] = None,
        samplingRatio: Optional[float] = None,
        verifySchema: bool = True,
    ) -> "DataFrame":
        if isinstance(schema, sqltypes.StructType):
            verify_func = sqltypes._make_type_verifier(schema) if verifySchema else lambda _: True

            @no_type_check
            def prepare(obj):
                verify_func(obj)
                return obj

        elif isinstance(schema, sqltypes.DataType):
            dataType = schema
            schema = sqltypes.StructType().add("value", schema)

            verify_func = (
                sqltypes._make_type_verifier(dataType, name="field value")
                if verifySchema
                else lambda _: True
            )

            @no_type_check
            def prepare(obj):
                verify_func(obj)
                return (obj,)

        else:

            def prepare(obj: Any) -> Any:
                return obj

        prepared = self.map(prepare)
        # Infer or apply the schema
        if schema is None or isinstance(schema, (list, tuple)):
            # Infer schema here
            struct = self._infer_schema(prepared, samplingRatio, schema)
            # Convert to the types
            converter = sqltypes._create_converter(struct)
            prepared = prepared.map(converter)
            if isinstance(schema, (list, tuple)):
                for i, name in enumerate(schema):
                    struct.fields[i].name = name
                    struct.names[i] = name
            schema = struct

        # Convert the data
        internal_rdd = prepared.map(schema.toInternal)
        # Map the output to the final schema
        pa_schema = _convert_spark_schema_to_pyarrow_schema(schema)

        def mapper(iter: Iterable[RecordBatch]):
            for b in iter:
                result = []
                rows = b.to_pylist()
                for r in rows:
                    val = loads(r["__bin_field__"])
                    # Zip the schema names and the values
                    result.append(dict(zip(map(lambda x: x.name, schema.fields), val)))
                yield RecordBatch.from_pylist(result, schema=pa_schema)

        return internal_rdd._df.mapInArrow(mapper, schema)

    toDF.__doc__ = RDD.toDF.__doc__

    def _infer_schema(
        self,
        rdd: "RDDAdapter",
        samplingRatio: Optional[float] = None,
        names: Optional[List[str]] = None,
    ) -> sqltypes.StructType:
        first = rdd.first()
        if isinstance(first, Sized) and len(first) == 0:
            raise ValueError("The first row in RDD is empty, can not infer schema")

        if samplingRatio is None:
            schema = self._df.sparkSession._inferSchemaFromList([first], names=names)
            if sqltypes._has_nulltype(schema):
                # Fetch 100 rows.
                data = rdd.take(100).collect()[1:]
                schema = self._df.sparkSession._inferSchemaFromList(data, names=names)
                if sqltypes._has_nulltype(schema):
                    # For cases like createDataFrame([("Alice", None, 80.1)], schema)
                    # we can not infer the schema from the data itself.
                    raise PySparkValueError(
                        error_class="CANNOT_DETERMINE_TYPE", message_parameters={}
                    )
        else:
            raise NotImplementedError("Sampling ratio is not supported")
        return schema

    def first(self):
        return RDDAdapter(self._df.limit(1), self._first_field).collect()[0]

    first.__doc__ = RDD.first.__doc__

    def take(self, num: int):
        return RDDAdapter(self._df.limit(num), self._first_field).collect()

    take.__doc__ = RDD.take.__doc__

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
