import json
import time,os,sys,argparse

# s3 relatve path
# from spark_demo.util import path_exists,make_date_key,get_dtstr_by_ts,get_ts8dtstr,get_time_part_by_ts
# github relative path
from util import path_exists,make_date_key,get_dtstr_by_ts,get_ts8dtstr,get_time_part_by_ts,del_s3_folder
from log_processors.metrics.metrics import get_metrics
import pyspark
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, when, lower, regexp_replace
from pyspark.sql.functions import col, from_json

sc = pyspark.SparkContext.getOrCreate()
sqlc = SQLContext(sc)


class MLMetrics():
    def __init__(self,runenv,start_time,start_date,end_date) -> None:
        self.runenv = runenv
        self.start_date = start_date
        self.end_date = end_date
    
    def run(self,cate = ""):
        start_ts = get_ts8dtstr(self.start_date+" 00:00:00")
        end_ts = get_ts8dtstr(self.end_date+" 00:00:00")
        diff_ts = end_ts - start_ts
        if diff_ts < 0:
            raise Exception("start_date is later than end_date.")
        cnt = int(diff_ts / 86400)

        paths=[]
        for idx in range(cnt+1):
            tts = start_ts + idx*86400
            tpart = get_time_part_by_ts(tts)
            tdate_key = make_date_key(tpart)
            rpath = "s3://hiretual-ml-data-test/dataplat_test/data/user_activity/%s" % (tdate_key,)
            if self.runenv == "prod":
                rpath = "s3://hiretual-ml-data/dataplat/data/user_activity/%s" % (tdate_key,)
            ret = path_exists(sc,rpath)
            if not ret:
                print("[MLMetrics]%s no exist" % (rpath,))
                continue
            paths.append(rpath)
            # df = sqlc.read.parquet(rpath)
        df = sqlc.read.option("mergeSchema", "false"
                        ).option("filterPushdown", "true"
                        ).parquet(*paths)
        metric_df = get_metrics(df)
        wbucket = "hiretual-ml-data-test"
        pre = "dataplat_test/data/tmp/%s_mid_metrics_v2" % (self.runenv,)
        wpath = "s3://%s/%s" % (wbucket,pre)
        del_s3_folder(wbucket,pre )
        print(paths,len(paths))
        print("wpath",wpath)
        metric_df.write.parquet(wpath)
        


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='emr submit')
    parser.add_argument('--start_time',help='submit start_time', required=True)
    parser.add_argument('--start_date',help='submit start_date', required=True)
    parser.add_argument('--end_date',help='submit end_date', required=True)
    parser.add_argument('--runenv',help='submit runenv', required=False)
    args = parser.parse_args()
    start_time = args.start_time
    start_date = args.start_date
    end_date = args.end_date
    runenv = args.runenv
    print("runenv",runenv)
    print("start_time",start_time)
    print("start_date",start_date)
    print("end_date",end_date)
    mlm = MLMetrics(runenv,start_time,start_date,end_date)
    mlm.run()