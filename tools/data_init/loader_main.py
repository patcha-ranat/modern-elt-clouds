import argparse
import os
import logging

from dotenv import load_dotenv

from utils.data_loader import MongoLoader, FirestoreLoader, DynamoDBLoader, KafkaLoader


# set basic logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
)

# set connections
load_dotenv()
MONGO_CONN_URI = os.environ["MONGO__CONN_URI"]


def entrypoint():
    parser = argparse.ArgumentParser(
        description="Data Loader for data soucres simulation"
    )

    parser.add_argument(
        "--load-type", type=str, required=True, choices=["batch", "streaming"]
    )

    parser.add_argument(
        "--destination",
        "-d",
        type=str,
        required=True,
        choices=["mongodb", "firestore", "dynamodb", "cosmosdb", "duckdb", "kafka"],
    )

    parser.add_argument(
        "--project-name", type=str, required=False, help="Cloud Project Name"
    )

    parser.add_argument(
        "--profile", type=str, required=False, help="AWS Profile Name"
    )

    parser.add_argument(
        "--database", type=str, required=False, help="Target NoSQL Database Name"
    )

    parser.add_argument(
        "--collection", type=str, required=False, help="Target NoSQL Collection Name"
    )

    parser.add_argument(
        "--table", type=str, required=False, help="Target DynamoDB Table Name"
    )

    parser.add_argument(
        "--bootstrap-server", type=str, required=False, help="e.g. 'localhost:9092'"
    )

    parser.add_argument(
        "--topic", type=str, required=False, help="e.g. 'topic_name' or 'topic-name' will load to 'topic-name' topic"
    )

    parser.add_argument(
        "--partition", type=str, required=False, help="partition column name"
    )

    parser.add_argument(
        "--data-path", type=str, required=False, help="relative path to json data file"
    )

    parser.add_argument(
        "--random-api", action="store_true", required=False, help="Use data from RandomUserAPI"
    )

    parser.add_argument(
        "--rows", type=int, required=False,
        help="Specify a number of rows to load to destination, unless load entire data file or 100 from RandomUserAPI request",
    )

    parser.add_argument(
        "--streaming-interval",
        type=float, required=False, default=0,
        help="interval in seconds to insert a row",
    )

    args = parser.parse_args()

    # Validate given arguments

    # Database requirement
    if (args.destination == "mongodb") and (args.database is None or args.collection is None):
        parser.error("destination mongodb requires --database and --collection")
    elif (args.destination == "firestore") and (args.project_name is None):
        parser.error("Firestore required --project-name argument")
    elif (args.destination == "dynamodb") and ((args.profile is None) or (args.table is None)):
        parser.error("DynamoDB required both --profile and --table arguments")
    elif (args.destination == "kafka") and ((args.bootstrap_server is None) or (args.topic is None)):
        parser.error("--destination kafka required both --bootstrap-server and --topic arguments")
    elif (args.destination == "kafka") and (args.load_type == "batch"):
        parser.error("--destination kafka do not accept --load-type batch mode")
    elif (args.partition is not None):
        # logging.warning("--partition is given, please make partition column is integer type")
        logging.warning("--partition for Kafka has not supported yet")

    # Local Data or Random API
    elif (args.data_path is None) and (args.random_api is False):
        parser.error("Required at least 1 argument, --data-path or --random-api")
    elif (args.data_path) and (args.random_api):
        parser.error("Only 1 argument is acceptable, --data-path or --random-api")
    
    # Batch or Streaming
    elif (args.load_type == "batch") and (args.streaming_interval):
        parser.error("--load-type do not accept --streaming-interval argument")
    else:
        pass
    
    # Process
    try:
        if args.destination == "mongodb":
            loader = MongoLoader(
                conn_uri=MONGO_CONN_URI, 
                database=args.database,
                collection=args.collection,
                data_path=args.data_path,
                random_api=args.random_api,
                rows=args.rows,
                streaming_interval=args.streaming_interval
            )
            if args.load_type == "batch":
                loader.bulk_insert()
            else: # streaming
                loader.one_insert()

        elif args.destination == "firestore":
            loader = FirestoreLoader(
                project=args.project_name, 
                database=args.database,
                collection=args.collection,
                data_path=args.data_path,
                random_api=args.random_api,
                rows=args.rows,
                streaming_interval=args.streaming_interval
            )
            if args.load_type == "batch":
                loader.bulk_insert()
            else: # streaming
                loader.one_insert()

        elif args.destination == "dynamodb":
            loader = DynamoDBLoader(
                profile=args.profile,
                table=args.table,
                data_path=args.data_path,
                random_api=args.random_api,
                rows=args.rows,
                streaming_interval=args.streaming_interval
            )
            if args.load_type == "batch":
                loader.batch_insert()
            else: # streaming
                loader.one_insert()
        elif args.destination == "cosmosdb":
            pass
        elif args.destination == "duckdb":
            pass
        elif args.destination == "kafka":
            loader = KafkaLoader(
                bootstrap_server=args.bootstrap_server,
                topic=args.topic,
                partition=args.partition,
                data_path=args.data_path,
                random_api=args.random_api,
                rows=args.rows,
                streaming_interval=args.streaming_interval
            )
            # manipulate streaming
            loader.send()

        logging.info("Process executed successfully! Data is fully loaded as specified.")

    except KeyboardInterrupt:
        logging.warning("Process is terminated by user. Data is loaded partially")
    except Exception as e:
        logging.exception(f"Unexpected {type(e)}, {e}")
        raise


if __name__ == "__main__":
    entrypoint()
