from typing import TYPE_CHECKING

import pandas
import pyarrow
from pyspark.sql.types import StructType, StructField, StringType

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

def to_json_conversion(self: "DataFrame") -> None:
    """
    This is a helper conversion that converts the input dataframe using
    `mapInArrow` to a row of JSON strings.
    :param self:
    :return:
    """
    import pyarrow as pa
    def converter(iterator):
        for batch in iterator:
            pdf : pandas.DataFrame = batch.to_pandas()
            json_df = pdf.to_json(orient='records', lines=True).strip()
            rows = json_df.split("\n")
            result_df = pandas.DataFrame(rows, columns=["value"])
            yield pyarrow.RecordBatch.from_pandas(result_df)
    return self.mapInArrow(converter,schema=StructType([StructField("value", StringType())]))