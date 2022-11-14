from tkinter import E
from pyspark import SparkConf
from pyspark.sql import SparkSession
import datetime,time
from pyspark.sql import SQLContext






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



def spark_hive(spark):
    tbl = "mysparktable"
    spark.sql("create table tmp select * from myhivetable" % (tbl,))
    df1=spark.sql("SHOW TABLES")
    df1.show()
    spark.table(tbl).show()
    sc = spark.sparkContext
    idx = int(time.time())
    df = sc.parallelize([(idx, 'a1.0'), ]).toDF(schema=['key', 'value'])
    df.write.insertInto(tbl, overwrite=False)
    spark.table(tbl).show()

def spark_hive_2(spark):
    sc = spark.sparkContext
    tbl = "myhivetable"
    df1 = spark.sql("SHOW TABLES")
    df1.show()
    spark.table(tbl).show()
    idx = int(time.time())
    df = sc.parallelize([(str(idx), 'a1.0'), ]).toDF(schema=['key', 'value'])
    df.write.insertInto(tbl, overwrite=False)
    spark.table(tbl).show()


import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='emr submit')
    parser.add_argument('--cate',help='submit cate', required=False)
    args = parser.parse_args()
    cate = args.cate
    if cate == "spark_hbase":
        spark = SparkSession.builder.appName("spark_hbase_job").getOrCreate()
        test_spark_hbase(spark)
    elif cate == "spark_hive":
        spark = SparkSession.builder.appName("spark_hive_job").enableHiveSupport().getOrCreate()
        sc = spark.sparkContext
        spark_hive(spark)
    elif cate == "spark_hive_2":
        spark = SparkSession.builder.appName("spark_hive_2_job").enableHiveSupport().getOrCreate()
        sc = spark.sparkContext
        spark_hive_2(spark)
    else:
        raise Exception("err cate")

