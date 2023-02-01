import pyspark
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, when, lower, regexp_replace
from pyspark.sql.functions import col, from_json


def get_metrics(df):
    df = df.filter(
        col("search_id").isNotNull()
    )
    
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
    final_df = final_df.filter(final_df["count"] > 10)
    final_df.show()
    print(final_df.count())
    

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
