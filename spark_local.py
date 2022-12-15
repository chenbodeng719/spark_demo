from gettext import find
from platform import java_ver
import findspark,os
# set hbase conf in spark_home/conf
findspark.init("/home/chenbodeng/app/spark-3.1.2-bin-hadoop3.2")
jar_str = ""
jar_str = "/home/chenbodeng/myproj/spark_demo/jar/hadoop_jars/*.jar"
jar_str = jar_str+",/home/chenbodeng/myproj/spark_demo/jar/aws_jars/*.jar"
jar_str = jar_str+",/home/chenbodeng/myproj/spark_demo/jar/spark_jars/*.jar"
# jar_str = jar_str+",/home/chenbodeng/myproj/spark_demo/jar/hbase/*.jar"
# jar_str = jar_str+",/home/chenbodeng/myproj/spark_demo/jar/hbase-connectors/spark/hsconn/*.jar"
findspark.add_jars(jar_str)
findspark._add_to_submit_args("--conf spark.driver.extraClassPath=/home/chenbodeng/app/spark-3.1.2-bin-hadoop3.2")
findspark._add_to_submit_args("--conf spark.executor.extraClassPath=/home/chenbodeng/app/spark-3.1.2-bin-hadoop3.2")

# print(jar_str)

import pyspark,time
from pyspark.sql import SQLContext,SparkSession

def hbase():
    sc = pyspark.SparkContext(appName="myAppName")
    print(pyspark.__version__)
    data_source_format = 'org.apache.hadoop.hbase.spark'


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
    tname = "mytable"
    tmap = "col0 STRING :key, col1 STRING f1:name"
    print(sc.getConf().getAll())
    sqlc = SQLContext(sc)
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
    df = sc.parallelize([('c', '1.0'), ('b', '2.0')]).toDF(schema=['col0', 'col1'])
    df.write.format("org.apache.hadoop.hbase.spark") \
    .option("hbase.columns.mapping",tmap) \
    .option("hbase.table", tname) \
    .option("hbase.spark.use.hbasecontext", False) \
    .save()

    df = sqlc.read.options(catalog=catalog).option("hbase.spark.pushdown.columnfilter", False).format(data_source_format).load()
    df.show()
    sc.stop()

def hive_hase():
    spark = SparkSession.builder.appName("spark_hive_2_job").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    tbl = "myhivetable_out"
    df1 = spark.sql("SHOW TABLES")
    df1.show()
    spark.table(tbl).show()
    sc.stop()

def myjob():
    sc = pyspark.SparkContext(appName="myjob",)
    sqlc = SQLContext(sc)
    data_source_format = 'org.apache.hadoop.hbase.spark'
    # aws s3 ls s3://htm-bi-data-prod/bi-collection-v2/year=2022/month=10/day=31/
    path = "s3a://htm-bi-data-test/bi-collection/year=2022/month=06/day=10/"
    df = sqlc.read.parquet(path)
    df.groupBy("type").count().sort(['count'],ascending=[False]).show(truncate=False)

    tname = "candidate"
    tmap = "uid STRING :key, col1 STRING f1:data"
    df = sqlc.read.format(data_source_format) \
        .option('hbase.table',tname) \
        .option('hbase.columns.mapping', tmap) \
        .option('hbase.spark.use.hbasecontext', False) \
        .option("hbase.spark.pushdown.columnfilter", False) \
        .load()
    df.createOrReplaceTempView("mycandidate")
    final_df = sqlc.sql("select * from mycandidate where uid in ('0007c08d-c15b-49ed-9f76-e1473b40e391') ")
    final_df.show()

def myjob_1():
    sc = pyspark.SparkContext(appName="myjob",)
    sqlc = SQLContext(sc)
    data_source_format = 'org.apache.hadoop.hbase.spark'

    tname = "candidate"
    tmap = "uid STRING :key, col1 STRING f1:data"
    df = sqlc.read.format(data_source_format) \
        .option('hbase.table',tname) \
        .option('hbase.columns.mapping', tmap) \
        .option('hbase.spark.use.hbasecontext', False) \
        .option("hbase.spark.pushdown.columnfilter", False) \
        .load()
    df.createOrReplaceTempView("mycandidate")
    final_df = sqlc.sql("select * from mycandidate where uid in ('0007c08d-c15b-49ed-9f76-e1473b40e391') ")
    final_df.show()


def myjob_2():
    sc = pyspark.SparkContext(appName="myjob",)
    sqlc = SQLContext(sc)
    data_source_format = 'org.apache.hadoop.hbase.spark'
    # aws s3 ls s3://htm-bi-data-prod/bi-collection-v2/year=2022/month=10/day=31/
    path = "s3a://htm-bi-data-test/bi-collection-v2/year=2022/month=11/day=15/"
    df = sqlc.read.parquet(path)
    # df.show()
    # df.filter(df.url.like("xxxx") ).show()
    # df.filter(df.event_name == "/login^^^^^^Next" ).show()
    # df.filter(df.event_name == "good_fit" ).show()
    df.filter(df.event_name == "ai_sourcing_task" ).show()
    # print(data_top)

def backlog_job():
    sc = pyspark.SparkContext(appName="backlog_job",)
    sqlc = SQLContext(sc)
    from pyspark.sql import functions as F
    path = "s3a://hiretual-ml-data-test/backlog_test/year=2022/month=11/day=23/"
    df = sqlc.read.parquet(path)
    # df.show()
    uniqueId = "requestId"
    reqDf = df.filter(df.requestResp == "" ).drop_duplicates(subset=[uniqueId])
    respDf = df.filter(df.requestBody == "" ).drop_duplicates(subset=[uniqueId])
    respDf = respDf.alias('respDf')
    reqDf = reqDf.alias('reqDf')
    finalDf = respDf.join(reqDf,getattr(respDf,uniqueId) ==  getattr(reqDf,uniqueId),"inner").select('respDf.*',F.col("reqDf.requestBody").alias("requestBodyReq"),F.col("reqDf.%s" % (uniqueId,)).alias("%sReq" % (uniqueId,)))
    finalDf.show()

def get_log():
    path = "s3a://hiretual-ml-data-test/dataplat_test/data/merge_log/year=2022/month=11/day=24/"
    sc = pyspark.SparkContext(appName="get_log",)
    sqlc = SQLContext(sc)
    from pyspark.sql import functions as F
    df = sqlc.read.parquet(path)
    print(df.count())




if __name__ == "__main__":
    myjob_1()
    # myjob_2()
    # backlog_job()
    # get_log()