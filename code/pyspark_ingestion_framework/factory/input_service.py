import json

import gcsfs
import boto3
from pyspark.sql import SparkSession, dataframe

from abstract.input_service import AbstractInputService
from factory.schema_service import SchemaService


class InputService(AbstractInputService):
    """
    Ingestion Reader for Data File and Schema Config
    """
    def __init__(self, spark: SparkSession, data_path: str, schema_config_path: str):
        """Class Entrypoint"""
        super().__init__()
        self.spark = spark
        self.data_path = data_path
        self.schema_config_path = schema_config_path
        self.schema_service: SchemaService

    def read_schema_config(self) -> None:
        if "gs://" in self.schema_config_path:
            gcs_file_system = gcsfs.GCSFileSystem(project="") # TODO: add project name
            with gcs_file_system.open(self.schema_config_path) as f:
                schema_config = json.load(f)

        elif "s3://" in self.schema_config_path:
            bucket_name = self.schema_config_path.split("/")[2]
            path = "/".join(self.schema_config_path.split("/")[3:])
            session = boto3.session.Session(profile_name="") # TODO: add profile name
            s3_client = session.resource("s3")
            content_object = s3_client.Object(bucket_name, path)
            schema_config = json.loads(content_object.get()["Body"].read().decode("utf-8"))

        else: # Local filesystem
            with open(self.schema_config_path, "r") as f:
                schema_config = json.load(f)
                f.close()

        self.schema_service = SchemaService(schema_config=schema_config)
        self.schema_service.process()

    def read_data(self) -> dataframe.DataFrame:
        if "gs://" in self.schema_config_path:
            pass
        elif "s3://" in self.schema_config_path:
            pass
        else: # Local filesystem
            df = self.spark.read\
                .schema(self.schema_service)\
                .format("json")\
                .load(self.data_path)
            
        return df

    def process(self):
        self.read_schema_config()
        self.read_data()
