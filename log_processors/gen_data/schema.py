from pyspark.sql.types import (StringType,
                               IntegerType,
                               StructField,
                               StructType,
                               ArrayType)


EVENT_TRACKING_SCHEMA = StructType([
    StructField("payload", StructType([
            StructField("candidate_records", ArrayType(StructType([
                StructField('candidate_id', StringType()),
                StructField('candidate_rank_info', StringType())
            ]))),
            StructField("project", StructType([
                StructField("project_id", StringType())
            ])),
            StructField("sourcing", StructType([
                StructField("sourcing_search_id", StringType()),
                StructField("sourcing_from", StringType()),
            ])),
        ])
    ),
    StructField("team_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_id", StringType()),
    StructField("inner_text", StringType()),
    StructField("event_name", StringType()),
    StructField("type", StringType()),
    StructField("module", StringType()),
    StructField("url", StringType()),
    StructField("timestamp", IntegerType())
])
