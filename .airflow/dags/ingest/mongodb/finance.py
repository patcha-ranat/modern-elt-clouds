import os
import datetime

from airflow.models import DAG
from airflow.models.variable import Variable
from airflow.models import Connection
from airflow.operators.empty import EmptyOperator
import dlt
from dlt.helpers.airflow_helper import PipelineTasksGroup

from utils import mongodb


DAG_ID = "ingestion_pipeline_mongodb_gcs"

# Default DAG Configuration
default_config = {
    "dlt_sources_mongo_collections": [
        "kde-finance-random-user",
        "kde-finance-cards-data",
        "kde-finance-mcc-codes",
        "kde-finance-train-fraud-labels",
        "kde-finance-transactions-data",
        "kde-finance-users-data",
    ],
}

# Overide DAG Configuration with Airflow Variables according to environments
environment_config = Variable.get(DAG_ID, {})
config = {**environment_config, **default_config}

# Global Configuration
gcs_bucket: dict = Variable.get("gcs", deserialize_json=True)
mongodb_database: str = Variable.get("mongodb_database")

# Airflow DAG
default_dag_args = {
    "owner": "kde",
    "email": "kde@kde.com",
    "doc_md": __doc__,
    "default_view": "grid",
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
    "depends_on_past": False
}

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2025, 1, 1),
    schedule="0 14 * * *",
    catchup=False,
    default_args=default_dag_args,
    tags=["ingestion", "dlt", "de"]
) as dag:
    start_task = EmptyOperator(task_id="start_task")
    
    extract_load_task = PipelineTasksGroup("extract_load", use_data_folder=False, wipe_local_data=True, use_task_logger=True)

    dlt_pipeline = dlt.pipeline(
        pipeline_name="extract_load_mongodb_gcs",
        destination="filesystem",
        dataset_name=gcs_bucket.get("landing"), # GCS BUCKET
        dev_mode=False,
    )

    extract_load_task.add_run(
        pipeline=dlt_pipeline,
        data=mongodb(
            connection_url=Connection.get_connection_from_secrets("mongo_default").get_uri(),
            database=mongodb_database,
            collection_names=config.get("dlt_sources_mongo_collections", []),
            write_disposition="replace",
        ),
        # decompose="serialize",
        decompose="parallel",
        trigger_rule="all_done",
        retries=0,
        provide_context=True,
    )
    
    end_task = EmptyOperator(task_id="end_task")

    start_task >> extract_load_task >> end_task
    