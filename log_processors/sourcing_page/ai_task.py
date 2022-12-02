"""
https://hiretual.atlassian.net/browse/PES-161
"""
import json

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from ..utils import row_to_dict


def get_tasks(df):
    return df.filter(df.event_name == "ai_sourcing_task")


@udf(returnType=StringType())
def get_task_id(row):
    return row.sourcing.sourcing_search_id


@udf(returnType=StringType())
def get_task_filter(row):
    _filter = row_to_dict(row.sourcing.sourcing_filters)
    _filter = json.dumps(_filter)
    return _filter

