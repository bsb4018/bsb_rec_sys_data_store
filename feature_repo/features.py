from datetime import timedelta

import pandas as pd

from feast import (Entity, Feature, FeatureView, RedshiftSource,
                   ValueType)

from feast import RedshiftSource

interaction_id = Entity(name = "interaction_id", value_type = ValueType.INT64)

interactions_source = RedshiftSource(
        query="select * from spectrum.interaction_features",
        event_timestamp_column="event_timestamp",
        created_timestamp_column = "created",
    )

interactions_fv = FeatureView(
    name = "interaction_features",
    entities = ["interaction_id"],
    ttl = timedelta(weeks=200),
    features = [
        Feature(name = "user_id", dtype =  ValueType.INT64),
        Feature(name = "course_id", dtype =  ValueType.INT64),
        Feature(name = "rating", dtype =  ValueType.FLOAT)
        ],
    batch_source = interactions_source
)


course_feature_id = Entity(name = "course_feature_id", value_type = ValueType.INT64)

courses_source = RedshiftSource(
        query="select * from spectrum.course_features",
        event_timestamp_column="event_timestamp",
        created_timestamp_column = "created",
    )

courses_fv = FeatureView(
    name = "course_features",
    entities = ["course_feature_id"],
    ttl = timedelta(weeks=200),
    features = [
    Feature(name = "course_id", dtype = ValueType.INT64),
    Feature(name = "course_name", dtype = ValueType.STRING),
    Feature(name = "course_tags", dtype = ValueType.STRING)
    ],
    batch_source = courses_source
)
