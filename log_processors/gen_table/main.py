

import json
import time,os,sys,argparse

# custom func
from util import path_exists,make_date_key,get_dtstr_by_ts,get_ts8dtstr,get_time_part_by_ts
from log_processors.utils import rank_info_schema

import pyspark
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, when, lower, regexp_replace
from pyspark.sql.functions import col, from_json

sc = pyspark.SparkContext.getOrCreate()
sqlc = SQLContext(sc)

def gen_ub_related_table(runenv,ts):
    tpart = get_time_part_by_ts(ts)
    tdate_key = make_date_key(tpart)
    rpath = "s3://htm-bi-data-test/bi-collection-v2/%s" % (tdate_key,)
    gen_click_table(rpath)
    gen_impression_table(rpath)



def gen_click_table(rpath,):
    ret = path_exists(sc,rpath)
    if not ret:
        print("[gen_click_table]%s no exist" % (rpath,))
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


def gen_impression_table(rpath,):
    ret = path_exists(sc,rpath)
    if not ret:
        print("[gen_impression_table]%s no exist" % (rpath,))
        return
    df = sqlc.read.parquet(rpath)
    new_df = df.filter((df.type == "impression") &
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
    new_df.show()


def gen_table(start_date):
    runenv = os.getenv("RUNENV",None)
    print("fuck",runenv)
    now_ts = int(time.time())
    last_date = get_dtstr_by_ts(now_ts-24*3600).split()[0]
    if not start_date:
        start_date = last_date
    start_ts = get_ts8dtstr(start_date+" 00:00:00")
    last_ts = get_ts8dtstr(last_date+" 00:00:00")
    diff_ts = last_ts - start_ts
    if diff_ts < 0:
        raise Exception("start_date is later than yesterday.")
    cnt = int(diff_ts / 86400)
    for idx in range(cnt+1):
        tts = start_ts + idx*86400
        gen_ub_related_table(runenv,tts,)
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='emr submit')
    parser.add_argument('--start_date',help='submit start_date', required=False)
    args = parser.parse_args()
    start_date = args.start_date
    gen_table(start_date)