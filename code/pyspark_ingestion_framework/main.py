import argparse

from pyspark.sql import SparkSession

from factory.argument_service import ArgumentService
from factory.schema_service import SchemaService
from factory.input_service import InputService
from utils.logging import set_logger


def entrypoint():
    logger = set_logger("Driver")
    app_name = "PySpark Ingestion Framework"

    parser = argparse.ArgumentParser(app_name)

    parser.add_argument(
        "--schema-path", type=str, required=True, 
        help="path to schema file"
    )

    parser.add_argument(
        "--input-path", type=str, required=True, 
        help="path to input file, can be either bucket+path or filesystem path"
    )

    parser.add_argument(
        "--output-path", type=str, required=True, 
        help="path to output file, can be either bucket+path or filesystem path"
    )

    parser.add_argument(
        "--dt", type=str, required=False, 
        help="execution date to filter"
    )

    parser.add_argument(
        "--write-mode", type=str, required=False, default="overwrite", choices=["overwrtie", "append"], 
        help="replace or delta, can be replaced by partition by config partition column"
    )

    parser.add_argument(
        "--format", type=str, required=False, default="parquet", choices=["parquet", "delta"], 
        help="output write format"
    )

    parser.add_argument(
        "--ingestion-mode", type=str, required=False, default="batch", choices=["batch", "streaming"], 
        help="spark streaming/batch mode"
    )

    args = parser.parse_args()

    logger.info("Read arguments successfully. Starting Ingestion Framework.")

    try:
        ArgumentService(args=args).process()
        
        # start spark session
        spark = SparkSession.builder \
                    .master("local[*]") \
                    .appName(app_name) \
                    .getOrCreate()
                    # .config("spark.some.config"). \

        InputService(spark=spark, data_path=args.input_path, schema_config_path=args.schema_path)

        # TransformService
        # OutputService
        # AuditService
        # IngestionProcessor

        logger.info("Ingestion Framework executed successfully.")
    
    except Exception as err:
        logger.error("Ingestion Framework executed unsuccessfully.")
        raise err
    
    finally:
        spark.stop()


if __name__ == '__main__':
    entrypoint()