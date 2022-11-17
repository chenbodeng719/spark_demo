from tkinter import E
from pyspark import SparkConf
from pyspark.sql import SparkSession
import datetime,time
from pyspark.sql import SQLContext
import argparse



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


def test_s3_parquet(spark):
    sc = spark.sparkContext
    sqlc = SQLContext(sc)
    path = "s3a://htm-bi-data-test/bi-collection-v2/year=2022/month=11/day=15/"
    df = sqlc.read.parquet(path)
    df.filter(df.event_name == "ai_sourcing_task" ).show()



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='emr submit')
    parser.add_argument('--cate',help='submit cate', required=False)
    args = parser.parse_args()
    cate = args.cate
    if cate == "spark_hbase":
        spark = SparkSession.builder.appName("spark_hbase").getOrCreate()
        test_spark_hbase(spark)
    elif cate == "s3_parquet":
        spark = SparkSession.builder.appName("spark_hbase_job").getOrCreate()
        test_s3_parquet(spark)
    else:
        raise Exception("err cate")

