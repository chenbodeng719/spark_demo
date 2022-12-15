

import json
import time,os,sys,argparse

# s3 relatve path
# from spark_demo.util import path_exists,make_date_key,get_dtstr_by_ts,get_ts8dtstr,get_time_part_by_ts
# github relative path
from util import path_exists,make_date_key,get_dtstr_by_ts,get_ts8dtstr,get_time_part_by_ts
from log_processors.gen_table.user_activity import filter_user_activity

import pyspark
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, when, lower, regexp_replace
from pyspark.sql.functions import col, from_json

sc = pyspark.SparkContext.getOrCreate()
sqlc = SQLContext(sc)

class GenTable():
    def __init__(self,runenv,start_time,start_date,) -> None:
        self.runenv = runenv
        self.start_date = start_date
        now_ts = get_ts8dtstr(start_time)
        self.last_date = get_dtstr_by_ts(now_ts-24*3600).split()[0]
        if not self.start_date:
            self.start_date = self.last_date

    def run(self,):
        start_ts = get_ts8dtstr(self.start_date+" 00:00:00")
        last_ts = get_ts8dtstr(self.last_date+" 00:00:00")
        diff_ts = last_ts - start_ts
        if diff_ts < 0:
            raise Exception("start_date is later than yesterday.")
        cnt = int(diff_ts / 86400)
        for idx in range(cnt+1):
            tts = start_ts + idx*86400
            self.gen_user_activity_table(runenv,tts,)

    def gen_user_activity_table(self,runenv,ts):
        tpart = get_time_part_by_ts(ts)
        tdate_key = make_date_key(tpart)
        rpath = "s3://htm-bi-data-test/bi-collection-v2/%s" % (tdate_key,)
        if runenv == "prod":
            rpath = "s3://htm-bi-data-prod/bi-collection-v2/%s" % (tdate_key,)
        ret = path_exists(sc,rpath)
        if not ret:
            print("[gen_ube_related_table]%s no exist" % (rpath,))
            return
        df = sqlc.read.parquet(rpath)
        user_activity_df = filter_user_activity(df)
        user_activity_df.show()
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='emr submit')
    parser.add_argument('--start_time',help='submit start_time', required=True)
    parser.add_argument('--start_date',help='submit start_date', required=False)
    parser.add_argument('--runenv',help='submit runenv', required=False)
    args = parser.parse_args()
    start_time = args.start_time
    start_date = args.start_date
    runenv = args.runenv
    print("start_time",start_time)
    gt = GenTable(runenv,start_time,start_date)
    gt.run()