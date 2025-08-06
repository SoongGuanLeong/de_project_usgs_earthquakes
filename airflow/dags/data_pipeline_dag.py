from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

SCRIPTS_DIR = "/opt/airflow/scripts"
DBT_PROJECT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="usgs_earthquake_pipeline",
    schedule=None,  # This DAG is not scheduled and must be triggered manually.
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data_engineering", "bigquery", "dbt"],
) as dag:
    
    # Task 1: Download raw JSON data from the USGS API.
    download_data_task = BashOperator(
        task_id="download_earthquake_data",
        bash_command=f"bash -c 'python {SCRIPTS_DIR}/1_download_earthquake_data.py'",
    )
    
    # Task 2: Organize the downloaded JSON files into year-based folders.
    organize_data_raw_task = BashOperator(
        task_id="organize_raw_json_files",
        bash_command=f"bash -c 'bash {SCRIPTS_DIR}/2_organize_data_raw.sh'",
    )
    
    # Task 3: Upload the raw JSON data to GCS.
    upload_raw_to_gcs_task = BashOperator(
        task_id="upload_raw_json_to_gcs",
        bash_command=f"bash -c 'bash {SCRIPTS_DIR}/3_upload_json_to_gcs.sh'",
    )
    
    # Task 4: Convert the JSON files to Parquet format.
    convert_to_parquet_task = BashOperator(
        task_id="convert_json_to_parquet",
        bash_command=f"bash -c 'python {SCRIPTS_DIR}/4_convert_to_parquet.py'",
    )
    
    # Task 5: Organize the parquet files into year-based folders.
    organize_data_parquet_task = BashOperator(
        task_id="organize_parquet_files",
        bash_command=f"bash -c 'bash {SCRIPTS_DIR}/5_organize_data_parquet.sh'",
    )
    
    # Task 6: Upload the parquet data to GCS.
    upload_parquet_to_gcs_task = BashOperator(
        task_id="upload_parquet_to_gcs",
        bash_command=f"bash -c 'bash {SCRIPTS_DIR}/6_upload_parquet_to_gcs.sh'",
    )
    
    # Task 7: Create the Bronze, Silver, and Gold datasets in BigQuery.
    create_bigquery_datasets_task = BashOperator(
        task_id="create_bigquery_datasets",
        bash_command=f"bash -c 'bash {SCRIPTS_DIR}/7_create_bq_datasets.sh'",
    )
    
    # Task 8: Create the external table in BigQuery for the raw data.
    create_external_table_task = BashOperator(
        task_id="create_bronze_external_table",
        bash_command=f"bash -c 'bash {SCRIPTS_DIR}/8_create_bq_bronze_table.sh'",
    )
    
    # Task 9: Install dbt project dependencies.
    run_dbt_deps = BashOperator(
        task_id="run_dbt_deps",
        bash_command=f"bash -c 'cd {DBT_PROJECT_DIR} && dbt deps'",
    )

    # Task 10: Run the dbt models for the Silver and Gold layers.
    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=f"bash -c 'cd {DBT_PROJECT_DIR} && dbt run'",
    )
    
    # Task 11: Run dbt tests to ensure data quality. This is the final step.
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=f"bash -c 'cd {DBT_PROJECT_DIR} && dbt test'",
    )

    # Define the task dependencies
    (
    download_data_task
    >> organize_data_raw_task
    >> upload_raw_to_gcs_task
    >> convert_to_parquet_task
    >> organize_data_parquet_task
    >> upload_parquet_to_gcs_task
    >> create_bigquery_datasets_task
    >> create_external_table_task
    >> run_dbt_deps
    >> run_dbt_models
    >> run_dbt_tests
    )
