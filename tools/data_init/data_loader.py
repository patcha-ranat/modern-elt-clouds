from typing import Any
import time
import logging
import json

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


class MongoLoader:
    def __init__(
        self,
        conn_uri: str,
    ):
        self.conn_uri = conn_uri
        self.client = MongoClient(self.conn_uri, server_api=ServerApi("1"))

    def bulk_insert(
        self, data_path: str, db: str, collection: str, rows: int | bool = False
    ):
        # get destination attributes
        mongo_db = self.client[db]
        mongo_collection = mongo_db[collection]

        # read data, considering rows
        with open(data_path, "r") as f:
            if rows:
                json_list: list[dict[str, Any]] = []
                for i, line in enumerate(f):
                    if i + 1 <= rows:
                        json_list.append(json.loads(line))
                    else:
                        break
            else:
                json_list: list[dict[str, Any]] = [json.loads(line) for line in f]
            f.close()

        # insert documents
        try:
            result = mongo_collection.insert_many(json_list)
            logging.info(
                f"Inserted {len(result.inserted_ids)} records to {db}.{collection}"
            )
        except Exception as e:
            raise e
        finally:
            logging.info("Operation is terminated")

    def one_insert(
        self,
        data_path: str,
        db: str,
        collection: str,
        rows: int | bool = False,
        streaming_interval: float = 0,
    ):
        # get destination attributes
        mongo_db = self.client[db]
        mongo_collection = mongo_db[collection]

        # read data & insert documents, considering rows and streaming_invertal
        with open(data_path, "r") as f:
            try:
                if rows:
                    for i, line in enumerate(f):
                        if i + 1 <= rows:
                            result = mongo_collection.insert_one(json.loads(line))
                            logging.info(
                                f"Inserted {result.inserted_id} to {db}.{collection}"
                            )
                            time.sleep(streaming_interval)
                        else:
                            break
                else:
                    for line in f:
                        result = mongo_collection.insert_one(json.loads(line))
                        logging.info(
                            f"Inserted {result.inserted_id} to {db}.{collection}"
                        )
                        time.sleep(streaming_interval)
            except Exception as e:
                raise e
            finally:
                f.close()
                logging.info("Operation is terminated")
