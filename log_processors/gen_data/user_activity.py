import sys

import pyspark
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, get_json_object, to_json, udf, flatten,explode
from pyspark.sql.types import StringType,IntegerType

sc = pyspark.SparkContext \
    .getOrCreate()


sqlc = SQLContext(sc)

CLICK_ACTIONS = set(['more_tags',
 'more_highlight',
 'more_experience',
 'more_social',
 'more_email',
 'more_education',
 'view_all_experience',
 'reveal_contact_info',
 'expand_experience',
 'expand_publication',
 'expand_highlight',
 'expand_education',
 'expand_skills',
 'expand_certification',
 'contact_info',
 'candidate_name',
 'external_link_linkedin',
 'external_link_googleScholar',
 'external_link_website',
 'reject'])

@udf(returnType=StringType())
def action_udf(inner_text, event_name, action_type):
    inner_text = inner_text.lower()
    action_type = action_type.lower()
    event_name = event_name.lower()
    if inner_text.find("good-fit") >= 0 or inner_text.find("good_fit") >= 0:
        if event_name == "bulk_move":
            return "bulk_move_good_fit"
        return "good_fit"
    elif inner_text.find("not-a-fit") >= 0 or inner_text.find("not_a_fit") >= 0:
        if event_name == "bulk_move":
            return "bulk_move_not_a_fit"
        return "not_a_fit"
    elif event_name in CLICK_ACTIONS and action_type == "click":
        return F"click_{event_name}"
    elif event_name== "bulk_move" and inner_text in {"contacted", "replied", "reject"}:
        return inner_text
    elif action_type == "impression":
        return "impression"
    return None

@udf(returnType=StringType())
def action_page_udf(module, event_name):
    if module.endswith("candidate-card"):
        return "candidate_list"
    elif module.endswith("talent_profile"):
        return "talent_profile"
    elif module.endswith("candidate-list"):
        return "candidate_list"
    elif event_name == "bulk_move":
        return "candidate_list"

@udf(returnType=StringType())
def page_category_udf(url):
    if url.endswith("candidates") and url.find("project-list"):
        return "project"
    elif url.endswith("search-result"):
        return "search_result"
    return None


def filter_user_activity(df):
    user_activity_df = df.withColumn(
        "candidate", explode(col("payload.candidate_records"))
    ).withColumn(
        "candidate_id", col("candidate.candidate_id")
    ).withColumn(
        "ranking_position", get_json_object(col("candidate.candidate_rank_info"), "$.searchSample")
    ).withColumn( 
        "model_version", get_json_object(col("candidate.candidate_rank_info"), "$.rankVersion")
    ).withColumn(
        "fe_position", get_json_object(col("candidate.candidate_rank_info"), "$.searchSample")
    ).withColumn(
        "action", action_udf(col("inner_text"), col("event_name"), col("type"))
    ).withColumn(
        "action_page", action_page_udf(col("module"), col("event_name"))
    ).withColumn(
        "page_category", page_category_udf(col("url"))
    ).filter(
        col("action").isNotNull() &
        col("page_category").isNotNull() &
        col("action_page").isNotNull()
    ).select(
        col("payload.sourcing.sourcing_search_id").alias("search_id"),
        col("client").alias("team_id"),
        col("payload.project.project_id").alias("project_id"),
        "user_id", 
        "event_id",
        "candidate_id",
        "fe_position", 
        "ranking_position",
        "action",
        "action_page",
        "page_category",
        "model_version",
        "timestamp"
    )

    user_activity_df = user_activity_df.withColumn("fe_position", user_activity_df["fe_position"].cast(IntegerType()))
    user_activity_df = user_activity_df.withColumn("ranking_position", user_activity_df["ranking_position"].cast(IntegerType()))
    return user_activity_df

# path = "s3://htm-bi-data-test/bi-collection-v2/year=2022/month=11/day=29/"
# df = sqlc.read.parquet(path)

# user_activity_df = filter_user_activity(df)
# user_activity_df.show()