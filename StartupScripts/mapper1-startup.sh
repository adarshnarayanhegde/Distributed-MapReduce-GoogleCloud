#!/bin/bash


gcloud compute --project adarsh-hegde firewall-rules create mapper1-rule --allow tcp:7002
gsutil cp gs://adarshs_storage_bucket_1/cloud_mapper.py /home/adarshhegdegef/cloud_mapper.py
chmod 777 /home/adarshhegdegef/cloud_mapper.py
python3 /home/adarshhegdegef/cloud_mapper.py 7002 0
