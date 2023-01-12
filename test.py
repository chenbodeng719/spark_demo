from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, explode, get_json_object, udf



def test_hbase_mget():
    spark = SparkSession.builder.appName("test_hbase_mget").getOrCreate()
    sc = spark.sparkContext
    sqlc = SQLContext(sc)
    data_source_format = 'org.apache.hadoop.hbase.spark'
    tname = "candidate"
    tmap = "uid STRING :key, oridata STRING f1:data"
    df = sqlc.read.format(data_source_format) \
        .option('hbase.table',tname) \
        .option('hbase.columns.mapping', tmap) \
        .option('hbase.spark.use.hbasecontext', False) \
        .option("hbase.spark.pushdown.columnfilter", True) \
        .load()
    # df = df \
    # .withColumn("position_title",get_json_object(col("oridata"), "$.basic.current_position.position_title") ) \
    # .filter(col("position_title") == "Business Consultant") \
    # .select(
    #     "uid",
    #     "position_title",
    #     "oridata",
    # )
    tlist = ["tiq_ed4f3ab4-d2d4-4108-83c8-408ff3cf5f2b"]
    # tlist = ["fcon_59124a3873d8f0.85770051", "tiq_58d1937c3a8550.54523267", "impt_5b53011b7d6a83.61364387", "impt_5b5b93e4c66628.23200830", "fu592c680b53c8f5.48096564", "fu589264bb564313.87757324", "fu589b42474b6013.97777442", "fu58aae034c0af85.13465164", "fu588b70d713aef5.64979322", "impt_5b695d2eeccb03.71995693"]
#     .filter(col("position_title") == "Business Consultant") \
    df = df \
    .filter(df.uid.isin(tlist)) \
    .withColumn("position_title",get_json_object(col("oridata"), "$.basic.current_position.position_title") ) \
    .select(
        "uid",
        "position_title",
        "oridata",
    )
                  
    df.show()


    
    



if __name__ == "__main__":
    test_hbase_mget()

