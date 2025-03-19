from typing import Any

from abstract.schema_service import AbstractSchemaService
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType, TimestampType, DecimalType
from utils.logging import set_logger


class SchemaService(AbstractSchemaService):
    """
    Read Table Schema configuration for transformation in ingestion process
    """
    def __init__(self, schema_config: dict):
        """Class Entrypoint"""
        super().__init__()
        self.logger = set_logger(__class__.__name__)
        
        # input
        self.schema_config: dict[str, Any] = schema_config

        # output schemas
        self.source_schema: StructType
        self.target_schema: StructType

        # output attributes
        self.source_dataset: str
        self.target_dataset: str
        self.table_name: str
        self.is_primary_key_exist: bool
        self.is_partition_column_exist: bool
        self.is_array_column_exist: bool
        self.primary_key: list
        self.partition_column: list
        # self.array_column: list

        # default attributes
        self.default_source_dataset: str = "bronze"
        self.default_target_dataset: str = "silver"

    def validate(self):
        """validate schema config"""
        self.logger.info("Starting Schema Configuration Validation.")
        
        attributes = self.schema_config.keys()
        
        # validation conditions
        if "dataset" not in attributes:
            self.logger.warning("'dataset' is not spcecified, default datasets will be applied for target table.")
            datasets = self.schema_config.get("dataset").keys()
            if "source" not in datasets:
                self.logger.warning(f"'dataset.source' is not specified, default dataset: '{self.default_source_dataset}' will be applied.")
            if "target" not in datasets:
                self.logger.warning(f"'dataset.target' is not specified, default dataset: '{self.default_target_dataset}' will be applied.")
        
        if "table_name" not in attributes:
            raise Exception("'table_name' attribute is not in the configuration file.")
        
        if "columns" not in attributes:
            raise Exception("'columns' attribute is not in the configuration file.")
        
        if "primary_key" not in attributes:
            self.logger.warning("'primary_key' is not specified, data enrichment and transformation will be affected.")
            self.is_primary_key_exist = False
        else:
            self.is_primary_key_exist = True
            # other check logic for primary key
        
        if "partition_column" not in attributes:
            self.logger.warning("'partition_column' is not specified, Spark performance will be affected")
            self.is_partition_column_exist = False
        else:
            self.is_partition_column_exist = True
            # other check logic for partition column

        if "array_column" not in attributes:
            self.logger.debug("'array_column' is not specified")
            self.is_array_column_exist = False
        else:
            self.is_array_column_exist = True

        # validate column level
        for column in self.schema_config.get("columns"):
            column_attributes = column.keys()
            if "target_column" not in column_attributes:
                raise Exception("'target_column' is not specified")
            if "source_column" not in column_attributes:
                raise Exception("'source_column' is not specified")
            if "column_type" not in column_attributes:
                raise Exception("'column_type' is not specified")
            if "nullable" not in column_attributes:
                raise Exception("'nullable' is not specified")
            
        self.logger("Validation Succeeded.")
        

    def generate(self):
        """generate schema from schema config to be used in spark process"""
        self.logger.info("Starting Generating Schema")
        schema_mapping: dict = {
            "str": StringType(),
            "int": IntegerType(),
            "float": DecimalType(),
            "bool": BooleanType(),
            "date": DateType(),
            "timestamp": TimestampType()
        }
        raw_schema: list = []
        target_schema: list = []
        
        for column in self.schema_config.get("columns"):
            raw_schema.append(StructField(name=column.get("source_column"), dataType=StringType(), nullable=column.get("nullable")))
            target_schema.append(StructField(name=column.get("target_column"), dataType=schema_mapping.get(column.get("column_type")), nullable=column.get("nullable")))

        self.source_schema = StructType(raw_schema)
        self.target_schema = StructType(target_schema)

        self.source_dataset = self.schema_config.get("source_dataset", self.default_source_dataset)
        self.target_dataset = self.schema_config.get("target_dataset", self.default_target_dataset)
        self.table_name = self.schema_config.get("table_name")
        self.primary_key = self.schema_config.get("primary_key")
        self.partition_column = self.schema_config.get("partition_column")
        # self.array_column = self.schema_config.get("array_column")

        self.logger("Schema Generation Succeeded.")

    def log(self):
        """Logging every output"""
        self.logger.debug(f"Read Schema Config: {self.schema_config}")
        self.logger.info(f"source_schema: {self.source_schema}")
        self.logger.info(f"target_schema: {self.target_schema}")
        self.logger.info(f"source_dataset: {self.source_dataset}")
        self.logger.info(f"target_dataset: {self.target_dataset}")
        self.logger.info(f"table_name: {self.table_name}")
        self.logger.info(f"primary_key: {self.primary_key}")
        self.logger.info(f"partition_column: {self.partition_column}")

    def process(self):
        """Class Main Process"""
        self.validate()
        self.generate()
        self.log()
