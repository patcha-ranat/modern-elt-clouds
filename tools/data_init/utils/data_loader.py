from typing import Any
from _io import TextIOWrapper
import time
import logging
import json

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from google.cloud import firestore

from utils.data_generator import DataGeneratorAPI


class BaseLoader:
    def __init__(self):
        pass

    def read_json_lines_file(
        self, 
        file: TextIOWrapper, 
        rows: int = None, 
        streaming_interval: float = 0,
        print_log: bool = False
    ):
        if rows:
            for i, line in enumerate(file):
                current_row = i + 1
                
                if current_row <= rows:
                    yield json.loads(line), current_row

                    if print_log:
                        logging.info(f"Processing row: {current_row}")
                    
                    time.sleep(streaming_interval)
                else:
                    break
        else:
            for i, line in enumerate(file):
                current_row = i + 1

                yield json.loads(line), current_row

                if print_log:
                    logging.info(f"Read row: {current_row}")

                time.sleep(streaming_interval)


class MongoLoader(BaseLoader):
    def __init__(
        self,
        conn_uri: str,
    ):
        super().__init__()
        self.conn_uri = conn_uri
        self.client = MongoClient(self.conn_uri, server_api=ServerApi("1"))

    def bulk_insert(
        self, 
        db: str, 
        collection: str, 
        data_path: str = None, 
        random_api: bool = False, 
        rows: int | bool = False
    ):
        # get destination attributes
        mongo_db = self.client[db]
        mongo_collection = mongo_db[collection]

        # read data, considering rows
        if data_path and random_api:
            raise Exception("'data_path' and 'random_api' parameters are given, only 1 paramater is acceptable")
        
        elif data_path:
            with open(data_path, "r") as f:
                documents: list[dict[str, Any]] = [document for document, _ in self.read_json_lines_file(file=f, rows=rows)]
                f.close()
        
        elif random_api:
            if not rows:
                rows = 100
            generator = DataGeneratorAPI(results=rows, format="json")
            documents: list[dict[str, Any]] = generator.get_data()
            # documents: list[dict[str, Any]] = = generator.get_data_with_schema()
        
        else:
            raise Exception("'data_path' or 'random_api' parameters are not given, the paramater is required")

        # insert documents
        try:
            logging.info(
                f"Inserting {len(documents)} records to {db}.{collection}"
            )
            result = mongo_collection.insert_many(documents)
            logging.info(
                f"Inserted {len(result.inserted_ids)} records to {db}.{collection}"
            )
        except Exception as e:
            raise e
        finally:
            logging.info("Operation has finished")

    def one_insert(
        self, 
        db: str, 
        collection: str, 
        data_path: str = None, 
        random_api: bool = False, 
        rows: int | bool = False,
        streaming_interval: float = 0,
    ):
        # get destination attributes
        mongo_db = self.client[db]
        mongo_collection = mongo_db[collection]

        # read data & insert documents, considering rows and streaming_invertal
        if data_path and random_api:
            raise Exception("'data_path' and 'random_api' parameters are given, only 1 paramater is acceptable")
        
        elif data_path:
            try: 
                with open(data_path, "r") as f:
                    for document, row_num in self.read_json_lines_file(file=f, rows=rows, streaming_interval=streaming_interval, print_log=True):
                        mongo_collection.insert_one(document)

                    logging.info(f"Inserted {row_num} rows to {db}.{collection}")

            except Exception as e:
                raise e

            finally:
                f.close()
                logging.info("Operation has finished")
        
        elif random_api:
            if not rows:
                rows = 100
            generator = DataGeneratorAPI(results=rows, format="json")
            documents: list[dict[str, Any]] = generator.get_data()
            try:
                for i, document in enumerate(documents):
                    current_doc_num = i + 1 
                    mongo_collection.insert_one(document)
                    
                    logging.info(f"Inserted {current_doc_num} to {db}.{collection}")
                    
                    time.sleep(streaming_interval)
                
                logging.info(f"Inserted {row_num} rows to {db}.{collection}")

            except Exception as e:
                raise e

            finally:
                logging.info("Operation has finished")
        else:
            raise Exception("'data_path' or 'random_api' parameters are not given, the paramater is required")


class FirestoreLoader(BaseLoader):
    def __init__(
        self,
        project,
        database
    ):
        super().__init__()
        self.project = project
        self.database = database
        self.client_db = firestore.Client(project=self.project, database=self.database)

    def bulk_insert(
        self,
        collection: str, 
        data_path: str = None, 
        random_api: bool = False, 
        rows: int | bool = False
    ):
        # get destination attributes
        client_collection = self.client_db.collection(collection)

        # read data, considering rows
        if data_path and random_api:
            raise Exception("'data_path' and 'random_api' parameters are given, only 1 paramater is acceptable")
        
        elif data_path:
            with open(data_path, "r") as f:
                logging.warning("Firestore not supported bulk inserting, streaming with 0 sec interval applied.")
                for document, row_num  in self.read_json_lines_file(file=f, rows=rows):
                    client_collection.add(document)
                f.close()

        elif random_api:
            if not rows:
                rows = 100
            generator = DataGeneratorAPI(results=rows, format="json")
            documents: list[dict[str, Any]] = generator.get_data()
            # documents: list[dict[str, Any]] = = generator.get_data_with_schema()
            logging.warning("Firestore not supported bulk inserting, streaming with 0 sec interval applied.")
            for document in documents:
                client_collection.add(document)
            row_num = rows
        
        else:
            raise Exception("'data_path' or 'random_api' parameters are not given, the paramater is required")

        # insert documents
        try:
            logging.info(
                f"Inserted {row_num} records to {self.database}.{collection}"
            )

        except Exception as e:
            raise e

        finally:
            logging.info("Operation has finished")

    def one_insert(
        self,
        collection: str, 
        data_path: str = None, 
        random_api: bool = False, 
        rows: int | bool = False,
        streaming_interval: float = 0,
    ):
        # get destination attributes
        client_collection = self.client_db.collection(collection)

        # read data & insert documents, considering rows and streaming_invertal
        if data_path and random_api:
            raise Exception("'data_path' and 'random_api' parameters are given, only 1 paramater is acceptable")
        
        elif data_path:
            try: 
                with open(data_path, "r") as f:
                    for document, row_num in self.read_json_lines_file(file=f, rows=rows, streaming_interval=streaming_interval, print_log=True):
                        client_collection.add(document)

                    logging.info(f"Inserted {row_num} rows to {self.database}.{collection}")

            except Exception as e:
                raise e

            finally:
                f.close()
                logging.info("Operation has finished")
        
        elif random_api:
            if not rows:
                rows = 100
            generator = DataGeneratorAPI(results=rows, format="json")
            documents: list[dict[str, Any]] = generator.get_data()
            try:
                for i, document in enumerate(documents):
                    row_num = i + 1 
                    client_collection.add(document)
                    
                    logging.info(f"Inserted record {row_num} to {self.database}.{collection}")
                    
                    time.sleep(streaming_interval)
                
                logging.info(f"Inserted {row_num} records to {self.database}.{collection}")

            except Exception as e:
                raise e
            finally:
                logging.info("Operation has finished")
        else:
            raise Exception("'data_path' or 'random_api' parameters are not given, the paramater is required")

        