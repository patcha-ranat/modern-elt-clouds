import argparse
import os
import logging

from dotenv import load_dotenv

from data_loader import MongoLoader



# set basic logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

# set connections
load_dotenv()
MONGO_CONN_URI = os.environ["MONGO__CONN_URI"]

def entrypoint():
    parser = argparse.ArgumentParser(description="Data Loader for data soucres simulation")

    parser.add_argument(
        "--load-type",
        type=str,
        required=True,
        choices=["batch", "streaming"]
    )

    parser.add_argument(
        "--destination",
        "-d",
        type=str,
        required=True,
        choices=["mongodb", "datastore", "dynamodb", "cosmosdb", "duckdb", "kafka"]
    )

    parser.add_argument(
        "--mongo-db",
        type=str,
        required=False,
        help="Target Mongo Database"
    )

    parser.add_argument(
        "--mongo-collection",
        type=str,
        required=False,
        help="Target Mongo Collection"
    )

    parser.add_argument(
        "--data-path",
        type=str,
        required=True,
        help="relative path to json data file"
    )

    parser.add_argument(
        "--rows",
        "-r",
        type=int,
        required=False,
        default=False,
        help="Specify a number of rows to load to destination, unless load entire data file or 100 from RandomUserAPI request"
    )

    parser.add_argument(
        "--streaming-interval",
        "-st",
        type=float,
        required=False,
        default=0,
        help="interval in seconds to insert a row"
    )

    args = parser.parse_args()

    # check arguments
    if (args.destination == "mongodb") and (args.mongo_db is None or args.mongo_collection is None):
        parser.error("destination mongodb requires --mongo-db and --mongo-collection.")

    try:
        if args.destination == "mongodb":
            loader = MongoLoader(conn_uri=MONGO_CONN_URI)
            if args.load_type == "batch":
                loader.bulk_insert(
                    data_path=args.data_path,
                    db=args.mongo_db,
                    collection=args.mongo_collection,
                    rows=args.rows
                )
            elif args.load_type == "streaming":
                loader.one_insert(
                    data_path=args.data_path,
                    db=args.mongo_db,
                    collection=args.mongo_collection,
                    rows=args.rows,
                    streaming_interval=args.streaming_interval
                )

        elif args.destination == "datastore":
            pass
        elif args.destination == "dynamodb":
            pass
        elif args.destination == "cosmosdb":
            pass
        elif args.destination == "duckdb":
            pass
        else:
            pass

        logging.info("Process executed successfully! Data is fully loaded as specified.")
        
    except KeyboardInterrupt:
        logging.warning("Process is terminated by user. Data is loaded partially")
    except Exception as e:
        logging.exception(f"Unexpected {type(e)}, {e}")
        raise

if __name__ == '__main__':
    entrypoint()
