#!/bin/bash

gcloud auth activate-service-account --key-file="/home/airflow/.gcp/my-free-tier-16-6-547bbd197f93.json"
gcloud config set project my-free-tier-16-6

bq mk --location=asia-southeast1 bronze
bq mk --location=asia-southeast1 silver
bq mk --location=asia-southeast1 gold
