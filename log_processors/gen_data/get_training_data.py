import sys
from typing import List, Text

import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.functions import (col, get_json_object,
                                   lit, udf)
from pyspark.sql.types import IntegerType, StringType

from log_processors.gen_data.user_activity import filter_user_activity
from log_processors.gen_data.schema import EVENT_TRACKING_SCHEMA

@udf(returnType=StringType())
def partition_by_last(candidate_id):
    return candidate_id[-1]


@udf(returnType=IntegerType())
def get_label(good_fit, click):
    # Good Fit
    if good_fit:
        return 4

    not_a_fit_missing = good_fit is None
    # Click
    if click and not_a_fit_missing:
        return 3

    if not not_a_fit_missing and not good_fit:
        # Not a Fit
        return 1

    # impression
    return 2


class TrainingDataGenerator:

    def __init__(self):
        sc = pyspark.SparkContext.getOrCreate()
        self.sqlc = SQLContext(sc)


    def get_aisourcing_training_labels(self, df):
        impressed_candidates = df.filter((df.action == "impression") |
                                        (df.action == "good_fit") |
                                        (df.action == "not_a_fit") |
                                        (df.action.like("click%"))).select(
                                            "user_id",
                                            "candidate_id").distinct().withColumn(
                                                "impressed", lit(True))

        good_fit_candidates = df.filter((col("action").like("%good_fit"))).select(
                                            "search_id", "user_id",
                                            "candidate_id").distinct().withColumn(
                                                "is_good_fit", lit(True))

        not_fit = df.filter((col("action").like("%not_a_fit"))).select(
                                "search_id", "user_id",
                                "candidate_id").distinct().withColumn(
                                    "is_good_fit", lit(False))

        click_candidates = df.filter(df.action.like("click%")).select(
            "user_id",
            "candidate_id").distinct().withColumn("is_clicked", lit(True))
        final_table = impressed_candidates.join(
            good_fit_candidates.union(not_fit),
            how="left",
            on=["user_id",
                "candidate_id"]).join(click_candidates,
                                    how='left',
                                    on=['user_id', 'candidate_id']).withColumn(
                                        "label",
                                        get_label(col("is_good_fit"),
                                                    col("is_clicked"))).select(
                                                        "user_id", "candidate_id",
                                                        "impressed", "is_good_fit",
                                                        "is_clicked", "label")

        search_candidates = df.filter(df.action.isNotNull() &
                                    df.search_id.isNotNull()).select(
                                        "search_id", "user_id", "team_id",
                                        "candidate_id").distinct().join(
                                            final_table,
                                            how="left",
                                            on=['user_id',
                                                'candidate_id']
                                    ).filter(col("label").isNotNull()
                                    ).withColumn("empty_id", partition_by_last(col("candidate_id")))

        return search_candidates

    def get_data_from_hbase(self, candidate_list: List[Text]):
        data_source_format = 'org.apache.hadoop.hbase.spark'
        tname = "candidate"
        tmap = "uid STRING :key, oridata STRING f1:data"

        df = self.sqlc.read.format(data_source_format) \
            .option('hbase.table',tname) \
            .option('hbase.columns.mapping', tmap) \
            .option('hbase.spark.use.hbasecontext', False) \
            .option("hbase.spark.pushdown.columnfilter", False) \
            .load()
        # print(df.count())
        #     .filter(col("position_title") == "Business Consultant") \
        df = df \
        .filter(df.uid.isin(candidate_list)) \
        .withColumn("profile", get_json_object(col("oridata"), "$.basic") ) \
        .select(
            col("uid").alias("candidate_id"),
            "profile",
        )

        return df

    def get_ai_sourcing_task(self, parquet):
        return parquet.filter(parquet.event_name == 'ai_sourcing_task'
        ).withColumn("sourcing",
                     col("payload.sourcing")
        ).select(col("sourcing.sourcing_filters.member0").alias("search_filter"),
                 col("sourcing.sourcing_search_id").alias("search_id")
        ).distinct()

    def run(self, parquet):
        df = filter_user_activity(parquet)
        training_labels = self.get_aisourcing_training_labels(df)
        ids = training_labels.rdd.map(lambda x: x.candidate_id).collect()#[:10]
        profile_df = self.get_data_from_hbase(ids)
        training_data = training_labels.join(profile_df,
                                        on=['candidate_id'],
                                        how='right')
        return training_data


if __name__ == "__main__":
    sc = pyspark.SparkContext \
        .getOrCreate()

    sc.addPyFile("s3://htm-test/chenbodeng/mytest/spark_demo/util.py")
    sc.addPyFile("s3://htm-test/chenbodeng/mytest/spark_demo/utils.py")

    sqlc = SQLContext(sc)

    from pyspark.sql.functions import col

    from util import path_exists


    def get_parquet_from_s3():
        path = "s3://htm-bi-data-test/bi-collection-v2/year=2023/month=01"
        ret = path_exists(sc,path)
        if ret:
            df = sqlc.read.schema(EVENT_TRACKING_SCHEMA
                         ).option("mergeSchema", "false"
                         ).option("filterPushdown", "true"
                         ).parquet(path)
            return df
        else:
            print("path no exist")

    parquet = get_parquet_from_s3()
    generator = TrainingDataGenerator()
    training_profiles = generator.run(parquet=parquet)
    training_profiles.show(10)
    training_searches = generator.get_ai_sourcing_task(parquet=parquet)
    training_searches.show(10)





