from typing import Any
from _io import TextIOWrapper
import time
import logging
import json
import uuid

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from google.cloud import firestore
import boto3
import boto3.session
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer

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
        database: str,
        collection: str,
        data_path: str = None,
        random_api: bool = False,
        rows: int = None,
        streaming_interval: float = 0
    ):
        super().__init__()
        self.conn_uri = conn_uri
        self.database = database
        self.collection = collection
        self.data_path = data_path
        self.random_api = random_api
        self.rows = rows
        self.streaming_interval = streaming_interval
        self.client = MongoClient(self.conn_uri, server_api=ServerApi("1"))

    def bulk_insert(self):
        # get destination attributes
        mongo_db = self.client[self.database]
        mongo_collection = mongo_db[self.collection]

        # read data, considering rows
        if self.data_path:
            with open(self.data_path, "r") as f:
                documents: list[dict[str, Any]] = [document for document, _ in self.read_json_lines_file(file=f, rows=self.rows)]
                f.close()
        
        else: # random_api
            if not self.rows:
                self.rows = 100
            generator = DataGeneratorAPI(results=self.rows, format="json")
            documents: list[dict[str, Any]] = generator.get_data()
            # documents: list[dict[str, Any]] = generator.get_data_with_schema()

        # insert documents
        try:
            logging.info(f"Inserting {len(documents)} records to {self.database}.{self.collection}")
            result = mongo_collection.insert_many(documents)
            logging.info(f"Inserted {len(result.inserted_ids)} records to {self.database}.{self.collection}")

        except Exception as e:
            raise e
        
        finally:
            logging.info("Operation has finished")

    def one_insert(self):
        # get destination attributes
        mongo_db = self.client[self.database]
        mongo_collection = mongo_db[self.collection]

        # read data & insert documents, considering rows and streaming_invertal
        if self.data_path:
            try: 
                with open(self.data_path, "r") as f:
                    for document, row_num in self.read_json_lines_file(file=f, rows=self.rows, streaming_interval=self.streaming_interval, print_log=True):
                        mongo_collection.insert_one(document)
                    f.close()

                    logging.info(f"Inserted {row_num} rows to {self.database}.{self.collection}")

            except Exception as e:
                raise e

            finally:
                logging.info("Operation has finished")
        
        else: # random_api
            if not self.rows:
                self.rows = 100
            generator = DataGeneratorAPI(results=self.rows, format="json")
            documents: list[dict[str, Any]] = generator.get_data()

            try:
                for i, document in enumerate(documents):
                    current_doc_num = i + 1 
                    mongo_collection.insert_one(document)
                    
                    logging.info(f"Inserted record {current_doc_num} to {self.database}.{self.collection}")
                    
                    time.sleep(self.streaming_interval)
                
                logging.info(f"Inserted {current_doc_num} rows to {self.database}.{self.collection}")

            except Exception as e:
                raise e

            finally:
                logging.info("Operation has finished")


class FirestoreLoader(BaseLoader):
    def __init__(
        self,
        project: str,
        database: str,
        collection: str,
        data_path: str = None,
        random_api: bool = False,
        rows: int | bool = False,
        streaming_interval: float = 0,
    ):
        super().__init__()
        self.project = project
        self.database = database
        self.collection = collection
        self.data_path = data_path
        self.random_api = random_api
        self.rows = rows
        self.streaming_interval = streaming_interval
        self.client_db = firestore.Client(project=self.project, database=self.database)

    def bulk_insert(self):
        # get destination attributes
        client_collection = self.client_db.collection(self.collection)
        logging.warning("Firestore not supported bulk inserting, streaming with 0 sec interval applied.")

        # read data, considering rows
        if self.data_path:
            try:
                with open(self.data_path, "r") as f:
                    for document, row_num  in self.read_json_lines_file(file=f, rows=self.rows):
                        client_collection.add(document)
                    f.close()

                logging.info(f"Inserted {row_num} records to {self.database}.{self.collection}")

            except Exception as e:
                raise e
            
            finally:
                logging.info("Operation has finished")

        else: # random_api
            try:
                if not self.rows:
                    self.rows = 100
                generator = DataGeneratorAPI(results=self.rows, format="json")
                documents: list[dict[str, Any]] = generator.get_data()
                
                for document in documents:
                    client_collection.add(document)
                row_num = self.rows

                logging.info(f"Inserted {row_num} records to {self.database}.{self.collection}")

            except Exception as e:
                raise e

            finally:
                logging.info("Operation has finished")

    def one_insert(self):
        # get destination attributes
        client_collection = self.client_db.collection(self.collection)

        # read data & insert documents, considering rows and streaming_invertal
        if self.data_path:
            try: 
                with open(self.data_path, "r") as f:
                    for document, row_num in self.read_json_lines_file(file=f, rows=self.rows, streaming_interval=self.streaming_interval, print_log=True):
                        client_collection.add(document)
                    f.close()

                    logging.info(f"Inserted {row_num} rows to {self.database}.{self.collection}")

            except Exception as e:
                raise e

            finally:
                logging.info("Operation has finished")
        
        else: # random_api
            if not self.rows:
                self.rows = 100
            generator = DataGeneratorAPI(results=self.rows, format="json")
            documents: list[dict[str, Any]] = generator.get_data()
            try:
                for i, document in enumerate(documents):
                    row_num = i + 1 
                    client_collection.add(document)
                    
                    logging.info(f"Inserted record {row_num} to {self.database}.{self.collection}")
                    
                    time.sleep(self.streaming_interval)
                
                logging.info(f"Inserted {row_num} records to {self.database}.{self.collection}")

            except Exception as e:
                raise e

            finally:
                logging.info("Operation has finished")


class DynamoDBLoader(BaseLoader):
    def __init__(
        self,
        profile: str,
        table: str,
        data_path: str = None,
        random_api: bool = False,
        rows: int | bool = False,
        streaming_interval: float = 0,
    ):
        super().__init__()
        self.profile = profile
        self.table = table
        self.data_path = data_path
        self.random_api = random_api
        self.rows = rows
        self.streaming_interval = streaming_interval
        
        session = boto3.session.Session(profile_name=self.profile)
        dynamodb = session.resource("dynamodb")
        self.table_client = dynamodb.Table(self.table)

    def batch_insert(self):
        # read data, considering rows
        if self.data_path:
            try:
                with self.table_client.batch_writer() as batch:
                    with open(self.data_path, "r") as f:
                        for document, row_num  in self.read_json_lines_file(file=f, rows=self.rows):
                            batch.put_item(Item=document)
                        f.close()

                logging.info(f"Inserted {row_num} records to '{self.table}' table with profile '{self.profile}'")

            except Exception as e:
                raise e
            
            finally:
                logging.info("Operation has finished")

        else: # random_api
            try:
                if not self.rows:
                    self.rows = 100
                generator = DataGeneratorAPI(results=self.rows, format="json")
                documents: list[dict[str, Any]] = generator.get_data()
                
                with self.table_client.batch_writer() as batch:
                    for document in documents:
                        batch.put_item(Item=document)
                row_num = self.rows

                logging.info(f"Inserted {row_num} records to '{self.table}' table with profile '{self.profile}'")

            except Exception as e:
                raise e

            finally:
                logging.info("Operation has finished")

    def one_insert(self):
        # read data & insert documents, considering rows and streaming_invertal
        if self.data_path:
            try: 
                with open(self.data_path, "r") as f:
                    for document, row_num in self.read_json_lines_file(file=f, rows=self.rows, streaming_interval=self.streaming_interval, print_log=True):
                        self.table_client.put_item(Item=document)
                    f.close()

                    logging.info(f"Inserted {row_num} records to '{self.table}' table with profile '{self.profile}'")

            except Exception as e:
                raise e

            finally:
                logging.info("Operation has finished")
        
        else: # random_api
            if not self.rows:
                self.rows = 100
            generator = DataGeneratorAPI(results=self.rows, format="json")
            documents: list[dict[str, Any]] = generator.get_data()
            try:
                for i, document in enumerate(documents):
                    row_num = i + 1 
                    self.table_client.put_item(Item=document)
                    
                    logging.info(f"Inserting record {row_num} to '{self.table}' table with profile '{self.profile}'")
                    
                    time.sleep(self.streaming_interval)
                
                logging.info(f"Inserted {row_num} records to '{self.table}' table with profile '{self.profile}'")

            except Exception as e:
                raise e

            finally:
                logging.info("Operation has finished")


class KafkaLoader(BaseLoader):
    def __init__(
        self,
        bootstrap_server: str,
        topic: str,
        partition: str,
        data_path: str = None,
        random_api: bool = False,
        rows: int = None,
        streaming_interval: float = 0
    ):
        super().__init__()
        self.bootstrap_server = bootstrap_server
        self.topic = self.transform_topic(topic_name=topic)
        self.partition = partition
        self.data_path = data_path
        self.random_api = random_api
        self.rows = rows
        self.streaming_interval = streaming_interval

    @staticmethod
    def transform_topic(topic_name: str) -> str:
        return topic_name.replace("_", "-")

    @staticmethod
    def ack(err, msg):
        """
        Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush().
        """

        if err is not None:
            logging.error(f"Message delivery failed: {err}")
        else:
            logging.info(f"Message record '{msg.key()}' delivered to '{msg.topic()}' partition [{msg.partition()}] at offset '{msg.offset()}'")

    def send(self):
        producer_config = {"bootstrap.servers": self.bootstrap_server}
        producer = Producer(**producer_config)

        string_serializer = StringSerializer("utf_8")

        # read data & insert documents, considering rows and streaming_invertal
        if self.data_path:
            try: 
                with open(self.data_path, "r") as f:
                    for record, row_num in self.read_json_lines_file(file=f, rows=self.rows, streaming_interval=self.streaming_interval):
                        producer.poll(self.streaming_interval)
                        producer.produce(
                            topic=self.topic,
                            key=string_serializer(str(uuid.uuid4())), # generate random key
                            # key=string_serializer(str(record.get("column_name"))),
                            value=str(record),
                            # partition=record.get(self.partition, 0),
                            on_delivery=self.ack
                        )
                    f.close()

                    logging.info(f"Message {row_num} records delivered to '{self.topic}'")

            except KeyboardInterrupt as err:
                logging.warning(f"{err}: Manually Terminated Kafka Session.")

            except Exception as e:
                raise e

            finally:
                logging.warning("\nFlushing records")
                producer.flush()
                logging.info("Operation has finished")
        
        else: # random_api
            if not self.rows:
                self.rows = 100
            generator = DataGeneratorAPI(results=self.rows, format="json")
            documents: list[dict[str, Any]] = generator.get_data()
            try:
                for i, record in enumerate(documents):
                    producer.poll(self.streaming_interval)
                    producer.produce(
                        topic=self.topic,
                        key=string_serializer(str(uuid.uuid4())), # generate random key
                        value=str(record),
                        # partition=record.get(self.partition, 0),
                        on_delivery=self.ack
                    )
                    
                    time.sleep(self.streaming_interval)
                
                row_num = i + 1
                logging.info(f"Message {row_num} records delivered to '{self.topic}'")

            except KeyboardInterrupt as err:
                logging.warning(f"{err}: Manually Terminated Kafka Session.")
            
            except Exception as e:
                raise e

            finally:
                logging.warning("\nFlushing records")
                producer.flush()
                logging.info("Operation has finished")
