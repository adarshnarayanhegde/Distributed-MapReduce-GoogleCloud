#!/bin/bash


gcloud compute --project adarsh-hegde firewall-rules create master-rule --allow tcp:7001
gsutil cp gs://adarshs_storage_bucket_1/cloud_master_v3.py /home/adarshhegdegef/cloud_master_v3.py

curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"
python3 get-pip.py
pip install --upgrade google-api-python-client
pip install oauth2client
pip install lxml
pip install requests
pip install textwrap3

chmod 777 /home/adarshhegdegef/cloud_master_v3.py
python3 /home/adarshhegdegef/cloud_master_v3.py

