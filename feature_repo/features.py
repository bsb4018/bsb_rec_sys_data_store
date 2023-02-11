from datetime import timedelta

import pandas as pd

from feast import (Entity, Feature, FeatureView, RedshiftSource,
                   ValueType, Field)
from feast.types import Int64,Float32,String
from feast import RedshiftSource

interaction_id = Entity(name = "interaction_id", value_type = ValueType.INT64)

interactions_source = RedshiftSource(
        name="rs_source_interactions",
        timestamp_field="event_timestamp",
        database="dev",
        schema="spectrum",
        table="interaction_features",
        query='SELECT * FROM "dev"."spectrum"."interaction_features"'
    )

interactions_fv = FeatureView(
    name = "interaction_features",
    entities = [interaction_id],
    ttl = timedelta(weeks=200),
    schema = [
        Field(name = "user_id", dtype =  Int64),
        Field(name = "course_id", dtype =  Int64),
        Field(name = "event", dtype =  Int64)
        ],
    source = interactions_source,
    online=False
)


user_feature_id = Entity(name = "user_feature_id", value_type = ValueType.INT64)

users_source = RedshiftSource(
        name = "rs_source_users",
        timestamp_field="event_timestamp",
        database="dev",
        schema="spectrum",
        table="user_features",
        query='SELECT * FROM "dev"."spectrum"."user_features"'
    )

users_fv = FeatureView(
    name = "user_features",
    entities = [user_feature_id],
    ttl = timedelta(weeks=200),
    schema = [
    Field(name = "prev_web_dev", dtype = Int64),
    Field(name = "prev_data_sc", dtype = Int64),
    Field(name = "prev_data_an", dtype = Int64),
    Field(name = "prev_game_dev", dtype = Int64),
    Field(name = "prev_mob_dev", dtype = Int64),
    Field(name = "prev_program", dtype = Int64),
    Field(name = "prev_cloud", dtype = Int64),    
    Field(name = "yrs_of_exp", dtype = Int64),
    Field(name = "no_certifications", dtype = Int64),
    Field(name = "user_id", dtype = Int64),
    ],
    source = users_source,
    online=False
)


'''
course_feature_id = Entity(name = "course_feature_id", value_type = ValueType.INT64)

courses_source = RedshiftSource(
        name = "rs_source_courses",
        timestamp_field="event_timestamp",
        database="dev",
        schema="spectrum",
        table="course_features",
        query='SELECT * FROM "dev"."spectrum"."course_features"'
    )

courses_fv = FeatureView(
    name = "course_features",
    entities = [course_feature_id],
    ttl = timedelta(weeks=200),
    schema = [
    Field(name = "course_id", dtype = Int64),
    Field(name = "course_name", dtype = String),
    Field(name = "course_tags", dtype = String)
    ],
    source = courses_source,
    online=True
)
'''