"""
GCP Firestore is not required to pre-create collection. 
This Script is meant to reset Firestore collections by deleting all contents.
"""

import os
import argparse

from dotenv import load_dotenv
from google.cloud import firestore


def delete_collection(client_collection, batch_size):
    if batch_size == 0:
        return

    print(f"Batch size: {batch_size}")

    total_deleted = 0
    while True:
        docs = client_collection.list_documents(page_size=batch_size)
        deleted = 0

        for doc in docs:
            doc.delete()
            deleted += 1
            total_deleted += 1

        print(f"Deleted {deleted} documents from {client_collection.id}.")

        if deleted >= batch_size:
            continue
        else:
            break

    print(f"Deleted Total {total_deleted} documents from {client_collection.id}.")

# Process

# Get Environment Variable
load_dotenv()
PROJECT = os.environ["GCP__PROJECT"]

# Get Input
parser = argparse.ArgumentParser("Simple Database Reset")
parser.add_argument("--database", required=True, help="Target Firestore Database")
parser.add_argument("--prefix", required=False, help="Target Firestore Collection Prefix")
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
client_db = firestore.Client(project=PROJECT, database=DB)

# Check if Reset collection
exists_collection: list[str] = [collection_name.id for collection_name in client_db.collections()]

if PREFIX:
    exists_collection = [collection for collection in exists_collection if collection.startswith(PREFIX)]

    if RESET_FLAG:
        print(f"Deleting collections with specified prefix: {exists_collection}")
        for collection in exists_collection:
            delete_collection(client_collection=client_db.collection(collection), batch_size=1000)
    else:
        print(f"Firestore don't required collections initialization. Aborted.")

elif RESET_FLAG:
    for collection in exists_collection:
        delete_collection(client_collection=client_db.collection(collection), batch_size=1000)
else:
    raise Exception("Please recheck given arguments.")

print("Operation Run Successfully!")
