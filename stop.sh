#! /bin/bash

cd infra/
terraform destroy -var="admin_password=anyPass1" -auto-approve
