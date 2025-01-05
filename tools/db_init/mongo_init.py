import os
import argparse

from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


# Process

# Get Environment Variable
load_dotenv()
CONN_URI = os.environ["MONGO__CONN_URI"]

# Get Input
parser = argparse.ArgumentParser("Simple Database Initialization")
parser.add_argument("--database", required=True, help="Target MongoDB Database")
parser.add_argument("--prefix", required=False, help="Target MongoDB Collection Prefix")
parser.add_argument("--reset", action="store_true", help="Use Reset Mode: Dropped All Collections)")
args = parser.parse_args()

DB = args.database
PREFIX = args.prefix
RESET_FLAG = args.reset

print("Input:")
print(f"Target Database: '{DB}'")
print(f"Target Prefix: '{PREFIX}'")
print(f"Reset Flag: {RESET_FLAG}")

# Create a new client and connect to the server
client = MongoClient(CONN_URI, server_api=ServerApi("1"))

db = client[DB]

# Check if Reset or Create collection
exists_collection = db.list_collection_names()

if RESET_FLAG and len(exists_collection) > 0:
    for collection in exists_collection:
        db.drop_collection(collection)
        print(f"Dropped Collection '{collection}' in database: '{DB}'")
elif PREFIX:
    collections = [
        f"{PREFIX}-finance-random-user",
        f"{PREFIX}-finance-cards-data",
        f"{PREFIX}-finance-mcc-codes",
        f"{PREFIX}-finance-train-fraud-labels",
        f"{PREFIX}-finance-transactions-data",
        f"{PREFIX}-finance-users-data",
    ]

    for collection in collections:
        if collection in exists_collection:
            db.drop_collection(collection)
            print(f"Dropped Collection '{collection}' in database: '{DB}'")

        db.create_collection(collection)
        print(f"Created Collection '{collection}' in database: '{DB}'")
elif len(exists_collection) == 0:
    print(f"Database: '{DB}' has no collection")
else:
    raise Exception("Required --prefix for creating collections.")

print("Operation Run Successfully!")
