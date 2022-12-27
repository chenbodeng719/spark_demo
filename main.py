from tkinter import E
from pyspark import SparkConf
from pyspark.sql import SparkSession
import datetime,time
from pyspark.sql import SQLContext
import argparse
from pyspark.sql import functions as F
from pyspark.sql.functions import col, explode, get_json_object, udf
import os
from util import get_time_part_by_ts,make_date_key
import json,logging


def test_spark_hbase(spark):
    sc = spark.sparkContext
    data_source_format = 'org.apache.hadoop.hbase.spark'
    # data_source_format = 'org.apache.spark.sql.execution.datasources.hbase'
    # ''.join(string.split()) in order to write a multi-line JSON string here.
    catalog = ''.join("""{
        "table":{"namespace":"default", "name":"mytable"},
        "rowkey":"key",
        "columns":{
            "col0":{"cf":"rowkey", "col":"key", "type":"string"},
            "col1":{"cf":"f1", "col":"name", "type":"string"}
        }
    }""".split())
    # Reading
    sqlc = SQLContext(sc)
    tname = "mytable"
    tmap = "col0 STRING :key, col1 STRING f1:name"
    print(sc.getConf().getAll())
    df = sqlc.read.format(data_source_format) \
        .option('hbase.table',tname) \
        .option('hbase.columns.mapping', tmap) \
        .option('hbase.spark.use.hbasecontext', False) \
        .load()
    df.show()
    # Writing 1
    # df = sc.parallelize([('a', '1.0'), ('b', '2.0')]).toDF(schema=['col0', 'col1'])
    # df.write.options(catalog=catalog).format(data_source_format).save()

    # Writing 2
    df = sc.parallelize([('a', '1.0'), ('b', '2.0')]).toDF(schema=['col0', 'col1'])
    df.write.format("org.apache.hadoop.hbase.spark") \
    .option("hbase.columns.mapping",tmap) \
    .option("hbase.table", tname) \
    .option("hbase.spark.use.hbasecontext", False) \
    .save()

    df = sqlc.read.options(catalog=catalog).option("hbase.spark.pushdown.columnfilter", False).format(data_source_format).load()
    df.show()


def test_hbase():
    spark = SparkSession.builder.appName("test_hbase").getOrCreate()
    sc = spark.sparkContext
    sqlc = SQLContext(sc)
    data_source_format = 'org.apache.hadoop.hbase.spark'
    tname = "candidate"
    tmap = "uid STRING :key, oridata STRING f1:data"
    df = sqlc.read.format(data_source_format) \
        .option('hbase.table',tname) \
        .option('hbase.columns.mapping', tmap) \
        .option('hbase.spark.use.hbasecontext', False) \
        .option("hbase.spark.pushdown.columnfilter", False) \
        .load()
    final_df = df \
    .withColumn("position_title",get_json_object(col("oridata"), "$.basic.current_position.position_title") ) \
    .filter(col("position_title") == "Business Consultant") \
    .select(
        "uid",
        "position_title",
        "oridata",
    )
    final_df.show()

def merge_backlog(runenv):
    spark = SparkSession.builder.appName("merge_backlog").getOrCreate()
    sc = spark.sparkContext
    sqlc = SQLContext(sc)
    ts = int(time.time())
    # tpart = get_time_part_by_ts(ts)
    tpart = get_time_part_by_ts(ts-23*3600)
    date_key = make_date_key(tpart)
    uniqueId = "requestId"
    rbucket = "hiretual-ml-data-test"
    wbucket = "hiretual-ml-data-test"
    rpath = "s3a://%s/%s/%s/%s" % (rbucket,"dataplat_test/data","backlog_job",date_key)
    wpath = "s3a://%s/%s/%s/%s" % (wbucket,"dataplat_test/data","merge_log",date_key)
    if runenv == "prod":
        rbucket = "hiretual-ml-data"
        wbucket = "hiretual-ml-data"
        rpath = "s3a://%s/%s/%s/%s" % (rbucket,"dataplat/data","backlog_job",date_key)
        wpath = "s3a://%s/%s/%s/%s" % (wbucket,"dataplat/data","merge_log",date_key)
        uniqueId = "ddTraceId"
    df = sqlc.read.parquet(rpath)
    reqDf = df.filter(df.requestResp == "" ).drop_duplicates(subset=[uniqueId])
    respDf = df.filter(df.requestBody == "" ).drop_duplicates(subset=[uniqueId])
    respDf = respDf.alias('respDf')
    reqDf = reqDf.alias('reqDf')
    finalDf = respDf.join(reqDf,getattr(respDf,uniqueId) ==  getattr(reqDf,uniqueId),"inner").select('respDf.*',F.col("reqDf.requestBody").alias("requestBodyReq"),F.col("reqDf.%s" % (uniqueId,)).alias("%sReq" % (uniqueId,)))
    finalDf.show()
    msg = "-------------wpath %s" % (wpath,)
    print("print: "+msg)
    finalDf.write.mode("overwrite").parquet(wpath)
    
    



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='emr submit')
    parser.add_argument('--cate',help='submit cate', required=False)
    parser.add_argument('--runenv', required=False)
    args = parser.parse_args()
    cate = args.cate
    if cate == "spark_hbase":
        spark = SparkSession.builder.appName("spark_hbase").getOrCreate()
        test_spark_hbase(spark)
    elif cate == "test_hbase":
        test_hbase()
    elif cate == "merge_backlog":
        merge_backlog(args.runenv)
    else:
        raise Exception("err cate")

