#!/bin/bash


gcloud compute --project adarsh-hegde firewall-rules create reducer2-rule --allow tcp:7006
gsutil cp gs://adarshs_storage_bucket_1/cloud_reducer.py /home/adarshhegdegef/cloud_reducer.py
chmod 777 /home/adarshhegdegef/cloud_reducer.py
python3 /home/adarshhegdegef/cloud_reducer.py 7006 1
