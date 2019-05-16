#!/bin/bash
# aws s3 cp s3://vault-mi9datascience/appnexus/appnexus_creds.sh appnexus_creds.sh --region ap-southeast-2
# source appnexus_creds.sh
cd ./app
python run.py

echo "Waiting for logs to flush to CloudWatch Logs..."
sleep 10  # twice the `buffer_duration` default of 5 seconds
