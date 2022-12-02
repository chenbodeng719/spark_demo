

import json
import time,os,sys,argparse

# custom func
from util import path_exists
from log_processors.utils import rank_info_schema

import pyspark
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, when, lower, regexp_replace
from pyspark.sql.functions import col, from_json

sc = pyspark.SparkContext.getOrCreate()
sqlc = SQLContext(sc)

def gen_click_tbl(runenv,tdate,):
    rpath = "s3://htm-bi-data-test/bi-collection-v2/year=2022/month=11/day=29/"
    ret = path_exists(sc,rpath)
    if ret:
        print("[gen_click_tbl]%s no exist" % (rpath,))
        return
    df = sqlc.read.parquet(rpath)
    click_df = df.filter((df.event_name.like("more_%")) |
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
    click_df.show()


def gen_table():
    runenv = os.getenv("RUNENV",None)
    gen_click_tbl(runenv,"")
    


if __name__ == "__main__":
    gen_table()