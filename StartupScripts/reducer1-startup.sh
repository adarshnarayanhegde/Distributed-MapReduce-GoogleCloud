#!/bin/bash


gcloud compute --project adarsh-hegde firewall-rules create reducer1-rule --allow tcp:7005
gsutil cp gs://adarshs_storage_bucket_1/cloud_reducer.py /home/adarshhegdegef/cloud_reducer.py
chmod 777 /home/adarshhegdegef/cloud_reducer.py
python3 /home/adarshhegdegef/cloud_reducer.py 7005 0
