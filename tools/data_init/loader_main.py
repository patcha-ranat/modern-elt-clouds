import argparse
import os
import logging

from dotenv import load_dotenv

from utils.data_loader import MongoLoader, FirestoreLoader


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
        "--db-name", type=str, required=False, help="Target Mongo Database"
    )

    parser.add_argument(
        "--collection-name", type=str, required=False, help="Target Mongo Collection"
    )

    parser.add_argument(
        "--data-path", type=str, required=False, help="relative path to json data file"
    )

    parser.add_argument(
        "--random-api", action="store_true", required=False, help="Use data from RandomUserAPI"
    )

    parser.add_argument(
        "--rows",
        "-r",
        type=int,
        required=False,
        default=False,
        help="Specify a number of rows to load to destination, unless load entire data file or 100 from RandomUserAPI request",
    )

    parser.add_argument(
        "--streaming-interval",
        "-st",
        type=float,
        required=False,
        default=0,
        help="interval in seconds to insert a row",
    )

    args = parser.parse_args()

    # Validate given arguments
    if (args.destination == "mongodb") and (args.db_name is None or args.collection_name is None):
        parser.error("destination mongodb requires --db-name and --collection-name")
    elif (args.data_path is None) and (args.random_api is False):
        parser.error("Required at least 1 argument, --data-path or --random-api")
    elif (args.data_path) and (args.random_api):
        parser.error("Required only 1 argument, --data-path or --random-api")
    elif (args.destination in ["firestore", "dynamodb", "cosmosdb"]) and (args.project_name is None):
        parser.error("Cloud Database required argument, --project-name")
    else:
        pass
    
    # Process
    try:
        if args.destination == "mongodb":
            loader = MongoLoader(conn_uri=MONGO_CONN_URI)
            if args.data_path:
                if args.load_type == "batch":
                    loader.bulk_insert(
                        data_path=args.data_path,
                        db=args.db_name,
                        collection=args.collection_name,
                        rows=args.rows,
                    )
                else: # streaming
                    loader.one_insert(
                        data_path=args.data_path,
                        db=args.db_name,
                        collection=args.collection_name,
                        rows=args.rows,
                        streaming_interval=args.streaming_interval,
                    )
            else: # random_api
                if args.load_type == "batch":
                    loader.bulk_insert(
                        random_api=args.random_api,
                        db=args.db_name,
                        collection=args.collection_name,
                        rows=args.rows,
                    )
                else: # streaming
                    loader.one_insert(
                        random_api=args.random_api,
                        db=args.db_name,
                        collection=args.collection_name,
                        rows=args.rows,
                        streaming_interval=args.streaming_interval,
                    )

        elif args.destination == "firestore":
            loader = FirestoreLoader(project=args.project_name, database=args.db_name)
            if args.data_path:
                if args.load_type == "batch":
                    loader.bulk_insert(
                        collection=args.collection_name,
                        data_path=args.data_path,
                        rows=args.rows,
                    )
                else: # streaming
                    loader.one_insert(
                        collection=args.collection_name,
                        data_path=args.data_path,
                        rows=args.rows,
                        streaming_interval=args.streaming_interval
                    )
            else: # random_api
                if args.load_type == "batch":
                    loader.bulk_insert(
                        collection=args.collection_name,
                        random_api=args.random_api,
                        rows=args.rows,
                    )
                else: # streaming
                    loader.one_insert(
                        collection=args.collection_name,
                        random_api=args.random_api,
                        rows=args.rows,
                        streaming_interval=args.streaming_interval
                    )

        elif args.destination == "dynamodb":
            pass
        elif args.destination == "cosmosdb":
            pass
        elif args.destination == "duckdb":
            pass
        elif args.destination == "kafka":
            pass

        logging.info(
            "Process executed successfully! Data is fully loaded as specified."
        )

    except KeyboardInterrupt:
        logging.warning("Process is terminated by user. Data is loaded partially")
    except Exception as e:
        logging.exception(f"Unexpected {type(e)}, {e}")
        raise


if __name__ == "__main__":
    entrypoint()
