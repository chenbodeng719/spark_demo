import os
import sys

import pyspark
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, when, lower, regexp_replace
from pyspark.sql.functions import col, explode,from_json, get_json_object, udf

sc = pyspark.SparkContext \
    .getOrCreate()


sqlc = SQLContext(sc)


def get_parquet_from_s3():
    path = "s3://htm-bi-data-test/bi-collection-v2/year=2022/month=12/day=30"
    df = sqlc.read.parquet(path)
    df.filter((df.event_name.like("more_%")) |
                      (df.event_name == "reveal_contact_info") |
                      (df.event_name == "candidate_name") |
                      (df.event_name == "external_link_linkedin") &
                      (df.url.like("%search-result") &
                      (df.type == "click"))
                    ).withColumn("candidate_records", explode("payload.candidate_records")
                    ).withColumn("action", when(df.event_name == "external_link_linkedin", "click_linkedin")
                                           .when(df.event_name == "candidate_name", "click_candidate_name")
                                           .when(df.event_name == "reveal_contact_info", "click_contact_info")
                                           .when(df.event_name.like("more_%"), regexp_replace("event_name","more","click"))
                    ).select(col("candidate_records.candidate_id").alias("candidate_id"),
                            col("user_id"),
                            col("payload.sourcing.sourcing_search_id"),
                            col("payload"),
                            "action"
                            )
    df.show()
    return df

if __name__ == "__main__":
    df = get_parquet_from_s3()
    wpath = "s3://hiretual-ml-data-test/data/chenbodeng/tutorial"
    df.write.parquet(wpath)
