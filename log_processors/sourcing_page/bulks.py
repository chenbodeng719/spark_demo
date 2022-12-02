"""https://hiretual.atlassian.net/browse/PES-165

https://hiretual.atlassian.net/browse/PES-166
https://hiretual.atlassian.net/browse/PES-167
https://hiretual.atlassian.net/browse/PES-168
"""
from pyspark.sql.functions import explode, when, lower
from pyspark.sql.functions import col, from_json
from ..utils import rank_info_schema

def get_select(df):
    return df.filter(((df.event_name == "select_all_candidates") |
                      (df.event_name == "deselect_all_candidates") |
                      (df.event_name == "select_candidate_card") |
                      (df.event_name == "deselect_candidate_card"))
                      & (df.url.like("%search-result"))
                    ).withColumn("candidate_records", explode("payload.candidate_records")
                    ).withColumn("rank_info", from_json("candidate_records.candidate_rank_info", rank_info_schema)
                    ).withColumn("action", when(df.event_name == "select_all_candidates", "bulk_select")
                                           .when(df.event_name == "deselect_all_candidates", "bulk_cancel")
                                           .when(df.event_name == "select_candidate_card", "single_select")
                                           .when(df.event_name == "deselect_candidate_card", "single_cancel")
                    ).select(col("candidate_records.candidate_id").alias("candidate_id"),
                            col("user_id"),
                            col("payload.sourcing.sourcing_search_id"),
                            col("rank_info.searchSample").alias("position"),
                            "action"
                            )


def get_bulk_move(df):
  return df.filter((df.event_name == "bulk_move")
                        & (df.url.like("%search-result"))
                      ).withColumn("candidate_records", explode("payload.candidate_records")
                      ).withColumn("rank_info", from_json("candidate_records.candidate_rank_info", rank_info_schema)
                      ).withColumn("bulk_move", when(lower(df.inner_text) == "good-fit", "good_fit")
                                                .when(lower(df.inner_text) == "not-a-fit", "not_a_fit")
                                                .otherwise(lower(df.inner_text))
                      ).select(col("candidate_records.candidate_id").alias("candidate_id"),
                              col("user_id"),
                              col("payload.sourcing.sourcing_search_id"),
                              col("rank_info.searchSample").alias("position"),
                              col("inner_text").alias("bulk_move"),
                              )