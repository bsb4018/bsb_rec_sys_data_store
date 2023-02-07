#! /bin/bash
python main.py
cd rec_sys_fs
feast apply
#CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")
#feast materialize-incremental $CURRENT_TIME
python src/components/data_sync.py