#!/bin/bash

gcloud auth activate-service-account --key-file="/home/airflow/.gcp/my-free-tier-16-6-547bbd197f93.json"
gcloud config set project my-free-tier-16-6

bq query --use_legacy_sql=false <<EOF
-- Drop old external table
EXECUTE IMMEDIATE """DROP EXTERNAL TABLE IF EXISTS bronze.usgs_earthquakes""";
-- Create external table
CREATE OR REPLACE EXTERNAL TABLE bronze.usgs_earthquakes
OPTIONS (
    format = 'PARQUET',
    uris = [
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/1990/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/1991/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/1992/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/1993/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/1994/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/1995/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/1996/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/1997/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/1998/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/1999/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2000/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2001/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2002/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2003/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2004/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2005/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2006/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2007/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2008/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2009/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2010/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2011/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2012/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2013/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2014/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2015/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2016/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2017/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2018/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2019/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2020/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2021/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2022/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2023/*.parquet',
        'gs://my-free-tier-16-6-usgs-earthquake/parquet/2024/*.parquet'
    ]
);
EOF
