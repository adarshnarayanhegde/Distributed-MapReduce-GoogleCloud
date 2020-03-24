#!/bin/bash

gcloud compute --project adarsh-hegde firewall-rules create datastore-rule --allow tcp:7000
gsutil cp gs://adarshs_storage_bucket_1/cloud_datastore.py /home/adarshhegdegef/cloud_datastore.py
chmod 777 /home/adarshhegdegef/cloud_datastore.py
python3 /home/adarshhegdegef/cloud_datastore.py

