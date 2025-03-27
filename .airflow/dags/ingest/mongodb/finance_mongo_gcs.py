import os
import datetime

from airflow.models import DAG
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
import dlt
from dlt.helpers.airflow_helper import PipelineTasksGroup

from utils import mongodb


DAG_ID = "ingestion_pipeline_mongodb_gcs"

# Default DAG Configuration
default_config = {
    "dlt_pipeline_name": "extract_load_mongodb_gcs",
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

# dlt Configuration
# def load_dlt_config():
#     """dlt.config and dlt.secrets have to be assign after dlt.pipeline is called to make config visible to dlt pipeline"""
#     dlt.config["destination.filesystem.bucket_url"] = f"s3://{s3_bucket.get('landing')}"
#     dlt.secrets["destination.filesystem.credentials.profile_name"] = os.getenv("AWS_PROFILE")
#     # dlt.secrets["<pipeline-name>.destination.filesystem.credentials.region_name"] = ""


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
    tags=["ingestion", "dlt", "de", "gcp"]
) as dag:
    start_task = EmptyOperator(task_id="start_task")
    
    extract_load_task = PipelineTasksGroup("extract_load_task", use_data_folder=False, wipe_local_data=True, use_task_logger=True)

    dlt_pipeline = dlt.pipeline( # ValueError: Please create your Pipeline instance after AirflowTasks are created.
        pipeline_name=config.get("dlt_pipeline_name"),
        destination="filesystem",
        dataset_name="mongodb", # path
        dev_mode=False, # Must be false if we decompose
    )

    # load_dlt_config()

    extract_load_task.add_run(
        pipeline=dlt_pipeline,
        data=mongodb(
            connection_url=BaseHook.get_connection("mongo_default").get_uri(), # this is equal to `get_connection_from_secrets`
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
    