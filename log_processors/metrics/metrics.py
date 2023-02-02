import pyspark
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, when, lower, regexp_replace
from pyspark.sql.functions import col, from_json


from pyspark.sql.functions import col,get_json_object, from_json, to_json,avg, udf,lit
from pyspark.sql.types import StringType,IntegerType,FloatType


@udf(returnType=IntegerType())
def gf_cnt_valid(gf_cnt):
    if gf_cnt is not None and gf_cnt > 0:
        return 1
    else:
        return 0
    
@udf(returnType=FloatType())
def gf_cnt_ratio(gf_cnt,total):
    if gf_cnt is None:
        return 0
    if gf_cnt >= total:
        return float(1)
    else:
        return gf_cnt/total

def get_metrics(df):
    df = df.filter(
        col("search_id").isNotNull()
    )
    total_cnt = df.groupBy("search_id").count().count()
#     valid
    valid_df = get_valid_df(df)
    valid_count = valid_df.count()
    
    df = valid_df.alias("tbl1").join(
        df.alias("tbl2"),valid_df.search_id == df.search_id,"inner"
    ).select("tbl2.*")
    

    sp_df = get_sp_df(df)
    search_cnt_df = get_search_cnt_df(df)
    gf25_df = get_gfpos_df(df,25)
    gf50_df = get_gfpos_df(df,50)
    gf_df = get_gfpos_df(df)

    final_df = sp_df.alias("tbl1").join(
        search_cnt_df.alias("tbl2"),sp_df.search_id == search_cnt_df.search_id,"left"
    ).select("tbl1.*","tbl2.count")
    final_df = final_df.alias("tbl1").join(
        gf25_df.alias("tbl2"),final_df.search_id == gf25_df.search_id,"left"
    ).select("tbl1.*",col("tbl2.count").alias("gf25_cnt"))
    final_df = final_df.alias("tbl1").join(
        gf50_df.alias("tbl2"),final_df.search_id == gf50_df.search_id,"left"
    ).select("tbl1.*",col("tbl2.count").alias("gf50_cnt"))
    final_df = final_df.alias("tbl1").join(
        gf_df.alias("tbl2"),final_df.search_id == gf_df.search_id,"left"
    ).select("tbl1.*",col("tbl2.count").alias("gf_cnt"))
#     final_df = final_df.filter(
#         final_df["count"] > 10
    final_df = final_df.withColumn(
        "gf25_cnt_valid",
        gf_cnt_valid(col("gf25_cnt"))
    ).withColumn(
        "gf25_cnt_ratio",
        gf_cnt_ratio(col("gf25_cnt"),lit(25))
    ).withColumn(
        "gf50_cnt_valid",
        gf_cnt_valid(col("gf50_cnt"))
    ).withColumn(
        "gf50_cnt_ratio",
        gf_cnt_ratio(col("gf50_cnt"),lit(50))
    ).withColumn(
        "gf_cnt_valid",
        gf_cnt_valid(col("gf_cnt"))
    )
    final_df = final_df.na.fill(value=0,subset=["gf25_cnt","gf50_cnt","gf_cnt","gf25_cnt_ratio","gf50_cnt_ratio"])
    
#     calculate basic metrics
    gf_ratio = final_df.filter(
        final_df["gf_cnt_valid"] > 0
    ).count() / valid_count
    gf25_ratio = final_df.filter(
        final_df["gf25_cnt_valid"] > 0
    ).count() / valid_count
    gf50_ratio = final_df.filter(
        final_df["gf50_cnt_valid"] > 0
    ).count() / valid_count
    gf25_avg_ratio = final_df.agg(avg(col("gf25_cnt_ratio"))).collect()[0][0]
    gf50_avg_ratio = final_df.agg(avg(col("gf50_cnt_ratio"))).collect()[0][0]
    
    metrics = {
        "search_total_cnt":total_cnt,
        "search_valid_cnt":valid_count,
        "gf_ratio":gf_ratio,
        "gf25_ratio":gf25_ratio,
        "gf50_ratio":gf50_ratio,
        "gf25_avg_ratio":gf25_avg_ratio,
        "gf50_avg_ratio":gf50_avg_ratio,
    }
    final_df.show()
    print(metrics)
    return final_df

#     spark.sql("select * from EMP e, DEPT d, ADD a " + \
#         "where e.emp_dept_id == d.dept_id and e.emp_id == a.emp_id") \
#         .show()
def get_valid_df(df):
    final_df = df.filter(
        df.action == 'impression' 
    )
    final_df = final_df.groupBy("search_id").count()
    final_df = final_df.filter(
        final_df["count"] >= 3  
    ).select(
        "search_id"
#         "count"
    )
#     final_df.show()
    return final_df
    

def get_sp_df(df):
    sp_df = df.dropDuplicates(
        ["search_id","project_id"]
    ).select(
        "search_id",
        "project_id"
    )
#     sp_df.show()
    return sp_df

def get_search_cnt_df(df):
    search_cnt_df = df.groupBy("search_id").count()
#     search_cnt_df.show()
    return search_cnt_df

def get_gfpos_df(df,pos = 0):
    gfpos_df = df.filter(
        df.action.isin(['good_fit','bulk_move_good_fit'] ) 
    )
    if pos > 0:
        gfpos_df = gfpos_df.filter(
            gfpos_df.ranking_position <= pos
        )
    gfpos_df = gfpos_df.groupBy(
        "search_id"
    ).count()
    
#     gfpos_df.show()
    return gfpos_df
