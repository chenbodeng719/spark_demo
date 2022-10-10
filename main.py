
import sys,os
from random import random
from operator import add
from tabnanny import verbose

from pyspark.sql import SparkSession

from pyspark import SparkConf
from pyspark.sql import SparkSession
import datetime


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

    dtstr = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M')
    df.groupBy("category").count().write.parquet("s3a://htm-test/chenbodeng/datatest/%s" % (dtstr,))



def test2(spark):
    path = 's3a://htm-test/chenbodeng/ret/100901.parquet'
    # path = "s3a://htm-bi-data-test/bi-collection/year=2022/month=06/day=10/"
    df = spark.read.parquet(path)
    df.show()
    # df.groupBy("category").count().show(truncate=False)
    # df.groupBy("category").count().write.parquet("s3a://htm-test/chenbodeng/ret/100901.parquet")

if __name__ == "__main__":
    spark = get_spark()
    test1(spark)

