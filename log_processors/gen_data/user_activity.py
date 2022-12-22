import re

import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, explode, get_json_object, udf
from pyspark.sql.types import StringType, IntegerType

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

@udf(returnType=StringType())
def get_project_id(project_id, url):
    if project_id is not None:
        return str(project_id)
    if url is None:
        return None
    new_search_pattern = r'sourcing_new/.(.*?)/search-result'
    project_id = re.search(new_search_pattern, url)
    if project_id:
        return project_id.group(1)
    project_search = r'project-list/.(.*?)/search-result'
    project_id = re.search(project_search, url)
    if project_id:
        return project_id.group(1)
    return None


@udf(returnType=IntegerType())
def set_recall_source(recall_source):
    if recall_source is None:
        return None
    recall_source = eval(recall_source)
    if not recall_source:
        return None
    if len(recall_source) == 1:
        if recall_source[0] == 0:
            # ES
            return 0
        elif recall_source[0] == 1:
            # VR
            return 1
        elif recall_source[0] == 2:
            # VR
            return 1
    elif len(recall_source) == 2:
        # Both
        return 2
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
        "recall_source", set_recall_source(get_json_object(col("candidate.candidate_rank_info"), "$.recallSource"))
    ).withColumn(
        "search_version", get_json_object(col("candidate.candidate_rank_info"), "$.searchVersion")
    ).withColumn(
        "action", action_udf(col("inner_text"), col("event_name"), col("type"))
    ).withColumn(
        "action_page", action_page_udf(col("module"), col("event_name"))
    ).withColumn(
        "page_category", page_category_udf(col("url"))
    ).withColumn(
        "project_id", get_project_id(col("payload.project.project_id"), col("url"))
    ).filter(
        col("action").isNotNull() &
        col("page_category").isNotNull() &
        col("action_page").isNotNull()
    ).select(
        col("payload.sourcing.sourcing_search_id").alias("search_id"),
        col("client").alias("team_id"),
        "project_id",
        "user_id",
        "event_id",
        "candidate_id",
        "fe_position",
        "ranking_position",
        "action",
        "action_page",
        "page_category",
        "model_version",
        "search_version",
        "recall_source",
        col("model_version").alias("experiment_type"),
        "timestamp",
    )

    user_activity_df = user_activity_df.withColumn("fe_position", user_activity_df["fe_position"].cast(IntegerType()))
    user_activity_df = user_activity_df.withColumn("ranking_position", user_activity_df["ranking_position"].cast(IntegerType()))
    return user_activity_df


