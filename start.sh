#! /bin/bash
python -m memory_profiler src/pipe/data_store_pipeline.py
cd infra/
terraform plan -var="admin_password=anyPass1"
terraform apply -var="admin_password=anyPass1" -auto-approve
aws redshift-data execute-statement \
    --region ap-south-1 \
    --cluster-identifier "bsb-4018-rec-sys-app-proj-redshift-cluster" \
    --db-user admin \
    --database dev --sql "create external schema spectrum from data catalog database 'dev' iam_role \
    'arn:aws:iam::487410058179:role/s3_spectrum_role' create external database if not exists;"
cd ..
cd feature_repo
feast apply
CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")
cd ..
python -m memory_profiler src/pipe/data_sync_pipeline.py
