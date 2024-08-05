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
from collections import defaultdict
from functools import reduce
from typing import (
    Any,
    TYPE_CHECKING,
    Iterable,
    Optional,
    Union,
    no_type_check,
    Tuple,
    List,
    Sized,
    Callable,
    TypeVar,
    Hashable,
    Generic,
    Dict,
)

import pandas
from pyspark.serializers import CloudPickleSerializer, CPickleSerializer, AutoBatchedSerializer
from pyspark.statcounter import StatCounter

import congruity.helper
from congruity.helper.rdd import WrappedIterator

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
U = TypeVar("U")
K = TypeVar("K", bound=Hashable)
V = TypeVar("V")
V1 = TypeVar("V1")
V2 = TypeVar("V2")
V3 = TypeVar("V3")

import pyarrow as pa
from pyarrow import RecordBatch
from pyspark import Row, RDD
import pyspark.sql.types as sqltypes
from pyspark.cloudpickle import loads, dumps
from pyspark.errors import PySparkValueError
from pyspark.util import fail_on_stopiteration

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


class RDDAdapter(Generic[T_co]):
    """This class implements the RDD methods of a PySpark DataFrame, but using the
    existing DataFrame operators. This is a workaround for the fact that Spark Connect
    does not support RDD operations"""

    BIN_SCHEMA = sqltypes.StructType(
        [sqltypes.StructField("__bin_field__", sqltypes.BinaryType(), True, {"serde": "true"})]
    )

    BIN_TUPLE_SCHEMA = sqltypes.StructType(
        [
            sqltypes.StructField("__bin_field_k__", sqltypes.BinaryType(), True, {"serde": "true"}),
            sqltypes.StructField("__bin_field_v__", sqltypes.BinaryType(), True, {"serde": "true"}),
        ]
    )

    PA_SCHEMA = pa.schema([pa.field("__bin_field__", pa.binary(), True, {"serde": "true"})])

    PA_TUPLE_SCHEMA = pa.schema(
        [
            pa.field("__bin_field_k__", pa.binary(), True, {"serde": "true"}),
            pa.field("__bin_field_v__", pa.binary(), True, {"serde": "true"}),
        ]
    )

    def __init__(self, df: "DataFrame", first_field: bool = False):
        self._df = df
        self._first_field = first_field

        # Compatibility attributes
        self._jrdd_deserializer = AutoBatchedSerializer(CPickleSerializer())

    def _memory_limit(self) -> int:
        # Dummy value
        return 1024 * 1024 * 512

    def collect(self: "RDDAdapter[T]") -> List[T]:
        data = self._df.collect()
        if self._first_field:
            assert len(self._df.schema.fields) == 1
            return [self._unnest_data(row[0]) for row in data]
        return data

    def collectAsMap(self: "RDDAdapter[Tuple[K, V]]") -> Dict[K, V]:
        return dict(self.collect())

    def isEmpty(self) -> bool:
        return len(self.take(1)) == 0

    def _unnest_data(self, data: Any) -> Any:
        if any(map(lambda x: isinstance(data, x), (str, Row, int, float, bool, type(None)))):
            return data
        if isinstance(data, bytearray):
            o = loads(data)
            return o
        raise NotImplementedError(f"Collecting Data type {type(data)} is not supported")

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

    def first(self: "RDDAdapter[T]") -> T:
        return RDDAdapter(self._df.limit(1), self._first_field).collect()[0]

    first.__doc__ = RDD.first.__doc__

    def take(self: "RDDAdapter[T]", num: int) -> List[T]:
        return RDDAdapter(self._df.limit(num), self._first_field).collect()

    take.__doc__ = RDD.take.__doc__

    def map(
        self: "RDDApapter[T]", f: Callable[[T], U], preservesPartitioning=None
    ) -> "RDDAdapter[U]":
        def func(iterator: Iterable[T]) -> Iterable[U]:
            return map(fail_on_stopiteration(f), iterator)

        # This is a diff to the regular map implementation because we don't have
        # access to mapPartitionsWithIndex
        return self.mapPartitions(func, preservesPartitioning)

    map.__doc__ = RDD.map.__doc__

    count = RDD.count
    count.__doc__ = RDD.count.__doc__

    sum = RDD.sum
    sum.__doc__ = RDD.sum.__doc__

    fold = RDD.fold
    fold.__doc__ = RDD.fold.__doc__

    keys = RDD.keys
    keys.__doc__ = RDD.keys.__doc__

    values = RDD.values
    values.__doc__ = RDD.values.__doc__

    glom = RDD.glom
    glom.__doc__ = RDD.glom.__doc__

    keyBy = RDD.keyBy
    keyBy.__doc__ = RDD.keyBy.__doc__

    reduce = RDD.reduce
    reduce.__doc__ = RDD.reduce.__doc__

    stats = RDD.stats
    stats.__doc__ = RDD.stats.__doc__

    stdev = RDD.stdev
    stdev.__doc__ = RDD.stdev.__doc__

    sampleStdev = RDD.sampleStdev
    sampleStdev.__doc__ = RDD.sampleStdev.__doc__

    sampleVariance = RDD.sampleVariance
    sampleVariance.__doc__ = RDD.sampleVariance.__doc__

    variance = RDD.variance
    variance.__doc__ = RDD.variance.__doc__

    aggregate = RDD.aggregate
    aggregate.__doc__ = RDD.aggregate.__doc__

    max = RDD.max
    max.__doc__ = RDD.max.__doc__

    min = RDD.min
    min.__doc__ = RDD.min.__doc__

    filter = RDD.filter
    filter.__doc__ = RDD.filter.__doc__

    histogram = RDD.histogram
    histogram.__doc__ = RDD.histogram.__doc__

    mean = RDD.mean
    mean.__doc__ = RDD.mean.__doc__

    variance = RDD.variance
    variance.__doc__ = RDD.variance.__doc__

    def groupBy(
        self: "RDDAdapter[T]",
        f: Callable[[T], K],
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[K], int] = lambda x: 1,
    ) -> "RDDAdapter[Tuple[K, Iterable[T]]]":
        # First transform the date into a tuple, in contrast to the regular map calls, we're going to
        # create the tuple as something that has two columns rather than one serialized value.
        converted = self.map(lambda x: x)
        transformer = lambda x: (f(x), x)

        def func(iterator: Iterable[T]) -> Iterable[U]:
            return map(fail_on_stopiteration(transformer), iterator)

        tuple_list = TupleAdapter(converted, func)
        return tuple_list.groupByKey(numPartitions, partitionFunc)

    groupBy.__doc__ = RDD.groupBy.__doc__

    def groupByKey(
        self: "RDDAdapter[Tuple[K, V]]",
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[K], int] = lambda x: 1,
    ) -> "RDDAdapter[Tuple[K, Iterable[V]]]":

        input_tuples = self
        if not isinstance(self, TupleAdapter):

            def extractor(x):
                # Extracting the k, v using unpacking will automatically raise an exception if the
                # number of values does not match.
                a, b = x
                return (a, b)

            def func(iterator: Iterable[Tuple[K, V]]) -> Iterable[Tuple[K, V]]:
                return map(fail_on_stopiteration(extractor), iterator)

            input_tuples = TupleAdapter(self, func)

        def transformer(df: pandas.DataFrame) -> pandas.DataFrame:
            batch = pa.RecordBatch.from_pandas(df, schema=RDDAdapter.PA_TUPLE_SCHEMA)

            # Generate the resulting aggregation
            result = defaultdict(list)
            for r in batch.to_pylist():
                # r has two columns k and v
                result[loads(r["__bin_field_k__"])].append(loads(r["__bin_field_v__"]))

            # Serialize back the result
            return pandas.DataFrame(
                [dumps(x) for x in result.items()],
                columns=[
                    "__bin_field__",
                ],
            )

        df = input_tuples._df.groupBy("__bin_field_k__").applyInPandas(
            transformer, schema=RDDAdapter.BIN_SCHEMA
        )
        return RDDAdapter(df, first_field=True)

    groupByKey.__doc__ = RDD.groupByKey.__doc__

    mapValues = RDD.mapValues
    mapValues.__doc__ = RDD.mapValues.__doc__

    def mapPartitions(
        self, f: Callable[[Iterable[T]], Iterable[U]], preservesPartitioning=False
    ) -> "RDDAdapter[U]":
        # Every pipeline becomes mapPartitions in the end. So we pass the current RDD as the
        # previous reference and the next transformation function.
        return Pipeline(self, f)

    mapPartitions.__doc__ = RDD.mapPartitions.__doc__


class TupleAdapter(RDDAdapter):
    """This is a helper class that takes an input RDD and converts it into a tupled RDD by
    creating an output DF that has two columns, one for the key and one for the value. The
    actual values are still encoded via cloudpickle."""

    def __init__(self, input: "RDDAdapter", f):
        super().__init__(input._df, input._first_field)
        mapper = self._build_mapper(f, input._first_field)
        self._df = input._df.mapInArrow(mapper, RDDAdapter.BIN_TUPLE_SCHEMA)
        self._first_field = True

    def _build_mapper(self, f, needs_conversion):
        # Fixed constants for the mapPartitions implementation.
        schema = RDDAdapter.PA_TUPLE_SCHEMA
        max_rows_per_batch = 1000
        return congruity.helper.generate_tuple_mapper(
            f, needs_conversion, schema, max_rows_per_batch
        )


class Pipeline(RDDAdapter):
    """The pipeline is an extension of the RDDAdapter that allows to pipeline multiple
    mapPartitions operations. This is useful to avoid the overhead of creating multiple
    plan execution nodes. Instead, we can create a single plan node that executes all the
    operations in a single pass."""

    def __init__(self, input: "RDDAdapter", f):
        # TODO check if this is ok
        super().__init__(input._df, input._first_field)

        if isinstance(input, Pipeline):
            source = input._prev_source
            self._prev_source = source

            prev_fun = input._prev_fun
            next_fun = lambda ite: f(prev_fun(ite))

            self._prev_fun = next_fun
            self._prev_first_field = input._prev_first_field
            first_field = input._prev_first_field
        else:
            # Cache the input DF and functions before mapping it.
            next_fun = f
            self._prev_source = input._df
            self._prev_fun = next_fun
            self._prev_first_field = input._first_field
            first_field = input._first_field
            source = input._df

        mapper = self._build_mapper(next_fun, first_field)
        # These are the output values of the operations, when a terminal operation is called
        # they will be evaluated.
        self._df = source.mapInArrow(mapper, RDDAdapter.BIN_SCHEMA)
        self._first_field = True

    def _build_mapper(self, f, needs_conversion):
        # Fixed constants for the mapPartitions implementation.
        schema = RDDAdapter.PA_SCHEMA
        max_rows_per_batch = 1000
        return congruity.helper.generate_partitions_mapper(
            f, needs_conversion, schema, max_rows_per_batch
        )
