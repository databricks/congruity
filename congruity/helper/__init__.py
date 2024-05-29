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

from typing import Iterable, Callable

import pyarrow as pa
from pyarrow import RecordBatch
from pyspark.cloudpickle import dumps

from congruity.helper.rdd import WrappedIterator


def generate_tuple_mapper(
    f: Callable, needs_conversion: bool, schema: pa.Schema, max_rows_per_batch: int = 1000
):
    def mapper(iter: Iterable[RecordBatch]):
        # the function that is passed to mapPartitions works the same way as the mapper. But
        # when next(iter) is called we have to send the converted batch instead of the raw
        # data.
        wrapped = WrappedIterator(iter, needs_conversion)
        result = []
        for kv in f(wrapped):
            # kv is a tuple with a key value pair.
            assert len(kv) == 2
            result.append({"__bin_field_k__": dumps(kv[0]), "__bin_field_v__": dumps(kv[1])})
            if len(result) > max_rows_per_batch:
                yield RecordBatch.from_pylist(result, schema=schema)
                result = []

        if len(result) > 0:
            yield RecordBatch.from_pylist(result, schema=schema)

    return mapper


def generate_partitions_mapper(
    f: Callable, needs_conversion: bool, schema: pa.Schema, max_rows_per_batch: int = 1000
):
    def mapper(iter: Iterable[RecordBatch]):
        # the function that is passed to mapPartitions works the same way as the mapper. But
        # when next(iter) is called we have to send the converted batch instead of the raw
        # data.
        wrapped = WrappedIterator(iter, needs_conversion)
        result = []
        for batch in f(wrapped):
            result.append({"__bin_field__": dumps(batch)})
            if len(result) > max_rows_per_batch:
                yield RecordBatch.from_pylist(result, schema=schema)
                result = []

        if len(result) > 0:
            yield RecordBatch.from_pylist(result, schema=schema)

    return mapper
