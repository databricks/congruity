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

from typing import Iterable

from pyarrow import RecordBatch
from pyspark import Row
from pyspark.cloudpickle import loads


class WrappedIterator(Iterable):
    """This is a helper class that wraps the iterator of RecordBatches as returned by
    mapInArrow and converts it into an iterator of the underlaying values."""

    def __init__(self, i: Iterable[RecordBatch], first_field=False):
        self._first_field = first_field
        self._iter = i
        self._current_batch = None
        self._current_idx = 0
        self._done = False

    def __next__(self):
        if self._current_batch is None or self._current_idx >= len(self._current_batch):
            self._current_idx = 0
            v: RecordBatch = next(self._iter)
            if self._first_field:
                self._current_batch = [loads(x["__bin_field__"]) for x in v.to_pylist()]
            else:
                self._current_batch = [Row(**x) for x in v.to_pylist()]

        result = self._current_batch[self._current_idx]
        self._current_idx += 1
        return result

    def __iter__(self):
        return self
