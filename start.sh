#! /bin/bash
python app.py
cd rec_sys_fs
feast apply
#CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")
#feast materialize-incremental $CURRENT_TIME