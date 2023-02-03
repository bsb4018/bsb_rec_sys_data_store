resource "aws_s3_bucket_acl" "feast_bucket" {
  bucket        = "${var.project_name}-bucket-75665"
  acl           = "private"
}

resource "aws_s3_object" "interaction_features_file_upload" {
  bucket = aws_s3_bucket_acl.feast_bucket.bucket
  key    = "interaction_features/table.parquet"
  source = "${path.module}/../data-feature/events_all.parquet"
}

resource "aws_s3_object" "courses_features_file_upload" {
  bucket = aws_s3_bucket_acl.feast_bucket.bucket
  key    = "course_features/table.parquet"
  source = "${path.module}/../data-feature/courses_data.parquet"
}


resource "aws_iam_role" "s3_spectrum_role" {
  name = "s3_spectrum_role"

  assume_role_policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "redshift.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF
}

data "aws_iam_role" "AWSServiceRoleForRedshift" {
  name = "AWSServiceRoleForRedshift"
}

resource "aws_iam_role_policy_attachment" "s3_read" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
  role       = aws_iam_role.s3_spectrum_role.name
}

resource "aws_iam_role_policy_attachment" "glue_full" {
  policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
  role       = aws_iam_role.s3_spectrum_role.name
}

resource "aws_iam_policy" "s3_full_access_policy" {
  name = "s3_full_access_policy"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "s3:*"
      ],
      "Effect": "Allow",
      "Resource": "*"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "s3-policy-attachment" {
  role       = aws_iam_role.s3_spectrum_role.name
  policy_arn = aws_iam_policy.s3_full_access_policy.arn
}

resource "aws_redshift_cluster" "feast_redshift_cluster" {
  cluster_identifier = "${var.project_name}-redshift-cluster"
  iam_roles = [
    data.aws_iam_role.AWSServiceRoleForRedshift.arn,
    aws_iam_role.s3_spectrum_role.arn
  ]
  database_name   = var.database_name
  master_username = var.admin_user
  master_password = var.admin_password
  node_type       = var.node_type
  cluster_type    = var.cluster_type
  number_of_nodes = var.nodes

  skip_final_snapshot = true
}

resource "aws_glue_catalog_table" "interaction_features_table" {
  name          = "interaction_features"
  database_name = var.database_name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket_acl.feast_bucket.bucket}/interaction_features/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "my-stream"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = 1
      }
    }
    
    columns {
      name = "user_id"
      type = "BIGINT"
    }
    columns {
      name = "event"
      type = "BIGINT"
    }
    columns {
      name = "course_id"
      type = "BIGINT"
    }
    columns {
      name = "event_timestamp"
      type = "TIMESTAMP"
    }
    columns {
      name = "interaction_id"
      type = "BIGINT"
    }
    
  }
}

resource "aws_glue_catalog_table" "course_features_table" {
  name          = "course_features"
  database_name = var.database_name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket_acl.feast_bucket.bucket}/course_features/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "my-stream"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = 1
      }
    }
    columns {
      name = "event_timestamp"
      type = "TIMESTAMP"
    }
    columns {
      name = "course_feature_id"
      type = "BIGINT"
    }
    columns {
      name = "course_id"
      type = "BIGINT"
    }
    columns {
      name = "course_name"
      type = "VARCHAR(150)"
    }
    columns {
      name = "course_tags"
      type = "VARCHAR(100)"
    }
  }
}