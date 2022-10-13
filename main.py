
import sys,os
from random import random
from operator import add
from tabnanny import verbose

from pyspark.sql import SparkSession

from pyspark import SparkConf
from pyspark.sql import SparkSession
import datetime,time
from pyspark.sql import SQLContext


def get_spark():
    spark = SparkSession.builder.appName('myjob').getOrCreate()
    return spark




def oper_s3(spark):
    sc = spark.sparkContext
    # A text dataset is pointed to by path.
    # The path can be either a single text file or a directory of text files
    path = "s3a://htm-test/chenbodeng/test"
    counts = sc.textFile(path)
    llist = counts.collect()
    # print line one by line
    for line in llist:
        print(line)
    counts.saveAsTextFile("s3a://htm-test/chenbodeng/ret1")


def test1(spark):
    path = 's3a://htm-bi-data-test/bi-collection/year=2022/month=06/day=10/1654819200006-100.parquet'
    # path = "s3a://htm-bi-data-test/bi-collection/year=2022/month=06/day=10/"
    df = spark.read.parquet(path)
    df.show()
    # df.groupBy("category").count().show(truncate=False)

    ts = int(time.time())
    dtstr = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')
    df.groupBy("category").count().write.parquet("s3a://htm-test/chenbodeng/datatest/%s" % (dtstr,))


def test_emr(spark):
    path = 's3a://htm-bi-data-test/bi-collection/year=2022/month=06/day=10/1654819200006-100.parquet'
    # path = "s3a://htm-bi-data-test/bi-collection/year=2022/month=06/day=10/"
    df = spark.read.parquet(path)
    df.show()
    # df.groupBy("category").count().show(truncate=False)

    ts = int(time.time())
    dtstr = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')
    new_path = "s3://htm-test/chenbodeng/datatest/%s" % (dtstr,)
    df.groupBy("category").count().write.parquet(new_path)
    df = spark.read.parquet(new_path)
    df.show()



def test2(spark):
    path = 's3a://htm-test/chenbodeng/ret/100901.parquet'
    # path = "s3a://htm-bi-data-test/bi-collection/year=2022/month=06/day=10/"
    df = spark.read.parquet(path)
    df.show()
    # df.groupBy("category").count().show(truncate=False)
    # df.groupBy("category").count().write.parquet("s3a://htm-test/chenbodeng/ret/100901.parquet")

def test_spark_hbase(spark):
    
    sc = spark.sparkContext
    sqlc = SQLContext(sc)

    data_source_format = 'org.apache.hadoop.hbase.spark'
    # data_source_format = 'org.apache.spark.sql.execution.datasources.hbase'

    df = sc.parallelize([('a', '1.0'), ('b', '2.0')]).toDF(schema=['col0', 'col1'])

    # ''.join(string.split()) in order to write a multi-line JSON string here.
    catalog = ''.join("""{
        "table":{"name":"mytable"},
        "rowkey":"key",
        "columns":{
            "col0":{"cf":"rowkey", "col":"key", "type":"string"},
            "col1":{"cf":"cf", "col":"col1", "type":"string"}
        }
    }""".split())


    # Writing
    df.write.options(catalog=catalog).format(data_source_format).save()
    # df.write.options(catalog=catalog).save()

    # Reading
    df = sqlc.read.options(catalog=catalog).format(data_source_format).load()
    df.show()

import argparse

if __name__ == "__main__":
    spark = get_spark()

    parser = argparse.ArgumentParser(description='emr submit')
    parser.add_argument('--cate',help='submit cate', required=False)
    args = parser.parse_args()
    cate = args.cate
    if cate == "spark_hbase":
        test_spark_hbase(spark)
    elif cate == "spark_hbase_shc":
        test_spark_hbase(spark)
    else:
        test_emr(spark)

