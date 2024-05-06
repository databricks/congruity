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

import json
from typing import TYPE_CHECKING

import pandas
import pyarrow as pa
from pyspark.sql.types import StructType, StructField, StringType

from congruity.rdd_adapter import RDDAdapter

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def to_json_conversion(self: "DataFrame") -> "RDDAdapter":
    """
    This is a helper conversion that converts the input dataframe using
    `mapInArrow` to a row of JSON strings.
    :param self:
    :return:
    """

    def converter(iterator):
        for batch in iterator:
            data = [{"value": json.dumps(x, separators=(",", ":"))} for x in batch.to_pylist()]
            yield pa.RecordBatch.from_pylist(data)

    df = self.mapInArrow(converter, schema=StructType([StructField("value", StringType())]))
    return RDDAdapter(df, first_field=True)
