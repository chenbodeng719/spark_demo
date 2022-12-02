"""https://hiretual.atlassian.net/browse/PES-164

PES-175
"""
from pyspark.sql.functions import explode
from pyspark.sql.functions import col, from_json

from ..utils import rank_info_schema


def get_single_feedback(df):
    return df.filter((df.event_name == "good_fit") |
                     (df.event_name == "not_a_fit")
                    ).withColumn("candidate_records", explode("payload.candidate_records")
                    ).withColumn("rank_info", from_json("candidate_records.candidate_rank_info", rank_info_schema)
                    ).select(col("candidate_records.candidate_id").alias("candidate_id"),
                            col("user_id"),
                            col("payload.sourcing.sourcing_search_id"),
                            col("rank_info.searchSample").alias("position"),
                            col("event_name").alias("feedback")
                            )