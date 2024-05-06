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
