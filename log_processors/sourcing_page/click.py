"""https://hiretual.atlassian.net/browse/PES-165

https://hiretual.atlassian.net/browse/PES-166
https://hiretual.atlassian.net/browse/PES-167
https://hiretual.atlassian.net/browse/PES-168
"""
from pyspark.sql.functions import explode, when, lower, regexp_replace
from pyspark.sql.functions import col, from_json
from ..utils import rank_info_schema

def get_click(df):
    return df.filter((df.event_name.like("more_%")) |
                      (df.event_name == "reveal_contact_info") |
                      (df.event_name == "candidate_name") |
                      (df.event_name == "external_link_linkedin") &
                      (df.url.like("%search-result") &
                      (df.type == "click"))
                    ).withColumn("candidate_records", explode("payload.candidate_records")
                    ).withColumn("rank_info", from_json("candidate_records.candidate_rank_info", rank_info_schema)
                    ).withColumn("action", when(df.event_name == "external_link_linkedin", "click_linkedin")
                                           .when(df.event_name == "candidate_name", "click_candidate_name")
                                           .when(df.event_name == "reveal_contact_info", "click_contact_info")
                                           .when(df.event_name.like("more_%"), regexp_replace("event_name","more","click"))
                    ).select(col("candidate_records.candidate_id").alias("candidate_id"),
                            col("user_id"),
                            col("payload.sourcing.sourcing_search_id"),
                            col("rank_info.searchSample").alias("position"),
                            "action"
                            )
