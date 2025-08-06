# Start from the official Airflow image
FROM apache/airflow:3.0.3

USER root

# --- Install the Google Cloud SDK ---
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee /etc/apt/sources.list.d/google-cloud-sdk.list && \
    apt-get update && \
    apt-get install -y google-cloud-sdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# --- FIX: Downgrade pyOpenSSL to <24.0.0 ---
# Pin to a version known to work with gsutil

# Switch to the 'airflow' user before running pip
USER airflow

RUN pip install --force-reinstall 'pyOpenSSL<24.0.0'

# Optional: Upgrade cryptography and related deps
RUN pip install --upgrade \
    cryptography \
    ndg-httpsclient \
    pyasn1

# Google Cloud Airflow provider
RUN pip install "apache-airflow-providers-google>=1.0.0"

# --- Install dbt-core and the database adapter ---
# This is a critical step to enable dbt commands within the container.
# Change 'dbt-postgres' to match your data warehouse (e.g., dbt-bigquery, dbt-redshift).
RUN pip install dbt-core dbt-bigquery

