from pyspark.sql.functions import explode
from pyspark.sql.functions import col, from_json

from ..utils import rank_info_schema

def get_impression(df):
    return df.filter((df.type == "impression") &
                        (df.url.like("%search-result")) &
                        (df.payload.impression.stay_time.isNotNull())
                        ).withColumn("candidate_records", explode("payload.candidate_records")
                        ).withColumn("rank_info", from_json("candidate_records.candidate_rank_info", rank_info_schema)
                        ).select(col("payload.impression.stay_time").alias("stay_time"),
                                col("candidate_records.candidate_id").alias("candidate_id"),
                                col("user_id"),
                                col("payload.sourcing.sourcing_search_id"),
                                col("rank_info.searchSample").alias("position"),
                                col("rank_info.rankVersion").alias("rank_version"))