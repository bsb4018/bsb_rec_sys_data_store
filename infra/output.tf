output "redshift_spectrum_arn" {
  value = aws_iam_role.s3_spectrum_role.arn
}

/*
output "course_features_table" {
  value = aws_glue_catalog_table.course_features_table.name
}
*/

output "user_features_table" {
  value = aws_glue_catalog_table.user_features_table.name
}

output "interaction_features_table" {
  value = aws_glue_catalog_table.interaction_features_table.name
}

output "redshift_cluster_identifier" {
  value = aws_redshift_cluster.feast_redshift_cluster.cluster_identifier
}