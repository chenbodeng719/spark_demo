import _json
from pyspark.sql.functions import udf
from pyspark.sql.types import (IntegerType, Row, StringType, StructField,
                               StructType, ArrayType)


def row_to_dict(nested_row):
    if isinstance(nested_row, Row):
        _dict = nested_row.asDict()
        return {k: row_to_dict(v) for k, v in _dict.items()}
    return nested_row

rank_info_schema = StructType([StructField("searchSample", IntegerType()),
                               StructField("sortByCount", IntegerType()),
                               StructField("relevance", StringType()),
                               StructField("totalNum", IntegerType()),
                               StructField("rankVersion", StringType()),
                               StructField("recallSource", ArrayType(StringType()),)
                               ])

@udf(returnType=StringType())
def rank_info_to_position(rank_info):
    return json.loads(rank_info)
