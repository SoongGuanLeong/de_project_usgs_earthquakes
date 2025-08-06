#!/bin/bash
gcloud auth activate-service-account --key-file="/home/airflow/.gcp/my-free-tier-16-6-547bbd197f93.json"
gcloud config set project my-free-tier-16-6
gsutil -m cp -r /opt/airflow/data/raw gs://my-free-tier-16-6-usgs-earthquake/raw/