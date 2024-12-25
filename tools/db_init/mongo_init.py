import os

from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Get Input
load_dotenv()
CONN_URI = os.environ["MONGO__CONN_URI"]
DB = input("Enter MongoDB Database Name: ")  # kde-db
PREFIX = input("Enter collection prefix: ")  # kde


# Process

# Create a new client and connect to the server
client = MongoClient(CONN_URI, server_api=ServerApi("1"))

# Check if Database exists
dbnames = client.list_database_names()
if DB in dbnames:
    db = client[DB]
else:
    raise Exception(f"Database: '{DB}' does not exist, Please create manually first.")

collections = [
    f"{PREFIX}_finance_cards_data",
    f"{PREFIX}_finance_mcc_codes",
    f"{PREFIX}_finance_train_fraud_labels",
    f"{PREFIX}_finance_transactions_data",
    f"{PREFIX}_finance_users_data",
]

exists_collection = db.list_collection_names()

for collection in collections:
    if collection in exists_collection:
        db.drop_collection(collection)
        print(f"Dropped Collection '{collection}' in databse: '{DB}'")

    db.create_collection(collection)
    print(f"Created Collection '{collection}' in databse: '{DB}'")

print("Operation Run Successfully!")
