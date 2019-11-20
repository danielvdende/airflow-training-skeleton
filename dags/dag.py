import airflow
import datetime
from airflow.contrib.operators.postgres_to_gcs_operator import (
    PostgresToGoogleCloudStorageOperator,
)
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator,
)
from airflow.models import DAG
from airflow_training.operators.http_to_gcs_operator import HttpToGcsOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator


project_id="airflowpublic-20631bd75d741210"
bucket_name="airflow-training-data"
currency="EUR"

args = {"owner": "Kris", "start_date": datetime.datetime(2019,9,20)}

dag = DAG(dag_id="usecase", default_args=args, schedule_interval="0 0 * * *")

pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="postgres_to_gcs",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket="{bucket}".format(bucket=bucket_name),
    filename="airflow-training-data/land_registry_price_paid_uk/{{ ds }}/properties_{}.json",
    postgres_conn_id="airflow-training-postgres",
    google_cloud_storage_conn_id="google_cloud_storage_default",
    dag=dag,
)

#https://europe-west2-gdd-airflow-training.cloudfunctions.net/function-1?date=2018-01-01&to=usd
http_to_gcs = HttpToGcsOperator(
    task_id="get_currency_" + currency,
    method="GET",
    endpoint="/airflow-training-transform-valutas?date={{ ds }}&to=" + currency,
    http_conn_id="airflow-training-currency-http",
    gcs_conn_id="google_cloud_storage_default",
    gcs_path="airflow-training-data/currency/{{ ds }}-" + currency + ".json",
    gcs_bucket="{bucket}".format(bucket=bucket_name),
    dag=dag,
)

dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="create_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id=project_id,
    num_workers=2,
    zone="europe-west4-a",
    dag=dag,
)

compute_aggregates = DataProcPySparkOperator(
    task_id="compute_aggregates",
    main="gs://{bucket}/build_statistics.py".format(bucket=bucket_name),
    cluster_name="analyse-pricing-{{ ds }}",
    arguments=[
        "gs://{bucket}/airflow-training-data/land_registry_price_paid_uk/*/*.json".format(bucket=bucket_name),
        ("gs://{bucket}/airflow-training-data/currency/{{{{ ds }}}}-" + currency + ".json").format(bucket=bucket_name),
        "gs://{bucket}/airflow-training-data/results/{{{{ ds }}}}/".format(bucket=bucket_name),
    ],
    dag=dag,
)

dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="delete_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id=project_id,
    dag=dag,
)

load_into_bigquery = DataFlowPythonOperator(
    task_id="load-into-bq",
    dataflow_default_options={
        "project": project_id,
        "region": "europe-west1",
        "staging_location": "gs://{bucket}/airflow-training-data/dataflow-staging".format(bucket=bucket_name),
        "temp_location": "gs://{bucket}/airflow-training-data/dataflow-temp".format(bucket=bucket_name),
    },
    py_file="gs://{bucket}/dataflow_job.py".format(bucket=bucket_name),
    options={
        "input": "gs://{bucket}/airflow-training-data/land_registry_price_paid_uk/{{{{ ds }}}}/".format(bucket=bucket_name),
        "table": "first_result_table",
        "dataset": "airflow_dataset",
    },
    dag=dag,
)

results_to_bigquery = GoogleCloudStorageToBigQueryOperator(
    task_id='results_to_bigquery',
    bucket=bucket_name,
    source_objects=[
        'airflow-training-data/results/{{ ds }}/part-*'
    ],
    destination_project_dataset_table='airflow_dataset.results_{{ ds_nodash }}',
    source_format='PARQUET',
    # schema_fields=schemas.gsob(),
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    google_cloud_storage_conn_id='google_cloud_storage_default',
    bigquery_conn_id='bigquery_default',
    autodetect=True,
    dag=dag,
)

[pgsl_to_gcs, http_to_gcs] >> dataproc_create_cluster >> compute_aggregates >> [ dataproc_delete_cluster, load_into_bigquery, results_to_bigquery ]
