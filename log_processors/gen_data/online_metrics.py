from pyspark.sql import Window
from pyspark.sql.functions import (col, collect_list, collect_set, count, size,
                                   struct)
from pyspark.sql.functions import udf, when
from pyspark.sql.types import FloatType
from sklearn.metrics import roc_auc_score


class Metrics:
    
    def get_non_duplicate_position(self, df):
        w = Window.partitionBy('search_id', "candidate_id")
        return df.withColumn(
                    "count_diff", size(collect_set(col("ranking_position")).over(w))
        ).filter(col("count_diff") <= 1)

    def get_has_impression(self, df):
        search_candidate_id = df.filter(
            col("search_id").isNotNull()
        ).select("search_id", "candidate_id").distinct()

        has_impression = df.filter(
            (col("action") == "impression") &
            (col("search_id").isNotNull())
        ).groupby("search_id", "candidate_id"
        ).agg(count("action").alias("action_count")
        ).withColumn(
            "has_impression", when(col("action_count") > 0, True)
        ).select("search_id", "candidate_id", "has_impression")
        
        has_impression = search_candidate_id.join(
            has_impression, 
            on=["search_id", "candidate_id"], 
        how="left")
        
        return has_impression
    
    def get_feedback(self, df):
    
        feedbacks = df.filter(
            (col("action").contains("fit")) &
            (col("search_id").isNotNull()) &
            (col("ranking_position").isNotNull())
        ).withColumn(
            "feedback", when(col("action").contains("good"), True)
                                .otherwise(False)
        ).select("search_id", 
                 "candidate_id", 
                 col("feedback").cast("float"), 
                 col("ranking_position").cast("float")).distinct()
    
        return feedbacks
    
    def get_auc(self, df):
        
        @udf(returnType=FloatType())
        def _get_auc(pairs):
            labels = [x[-1] for x in pairs]
            prediction = [1 / (x[0] + 1) for x in pairs]
            if all(labels) or not any(labels):
                return -1.0
            return float(roc_auc_score(labels, prediction))
        
        feedbacks = self.get_feedback(df)
        impression = self.get_has_impression(df)
        auc = feedbacks.join(impression, 
                             on=['search_id', "candidate_id"], 
                             how='left'
                ).withColumn("has_impression", 
                             when(col("has_impression").isNotNull(), True)
                             .otherwise(False)
                ).withColumn("rankings", 
                             struct(col("ranking_position"), col("feedback"))
                ).groupBy(["search_id"]
                ).agg(collect_list("rankings").alias("for_aucs")
                ).withColumn("auc", _get_auc(col("for_aucs"))
                ).select("search_id",
                         "auc")
        return auc
    

