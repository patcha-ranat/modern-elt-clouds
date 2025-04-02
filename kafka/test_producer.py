"""
Python-Kafka Producer
"""

import argparse

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient

# from ..tools.data_init.utils.data_loader import BaseLoader
from _io import TextIOWrapper
import json
import time
import logging


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
                
                if i+1 <= rows:
                    yield json.loads(line), i+1

                    if print_log:
                        logging.info(f"Processing row: {i+1}")
                    
                    time.sleep(streaming_interval)
                else:
                    break
        else:
            for i, line in enumerate(file):

                yield json.loads(line), i+1

                if print_log:
                    logging.info(f"Read row: {i+1}")

                time.sleep(streaming_interval)


def ack(err, msg):
    """
    Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush().
    """

    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message record '{msg.key()}' delivered to '{msg.topic()}' [{msg.partition()}] at offset '{msg.offset()}'")

def read_schema(schema_path: str, output_type: str) -> str | dict:
    with open(schema_path, "r") as f:
        schema_dict = json.load(f)

    if output_type == "dict":
        return schema_dict
    elif output_type == "str":
        return json.dumps(schema_dict) 
    else:
        raise Exception(f"output_type: {output_type} is not supported")

def model_to_dict(data: dict, context: SerializationContext):
    """
    Function to Transform Data Model to a dict required for Schema Registry.
    But we don't have data model class, this is unnecessary.
    """
    return dict(
        id                      = data.get("id"),
        client_id               = data.get("client_id"),
        card_brand              = data.get("card_brand"),
        card_type               = data.get("card_type"),
        card_number             = data.get("card_number"),
        expires                 = data.get("expires"),
        cvv                     = data.get("cvv"),
        has_chip                = data.get("has_chip"),
        num_cards_issued        = data.get("num_cards_issued"),
        credit_limit            = data.get("credit_limit"),
        acct_open_date          = data.get("acct_open_date"),
        year_pin_last_changed   = data.get("year_pin_last_changed"),
        card_on_dark_web        = data.get("card_on_dark_web")
    )

def main(args):
    # Producer
    producer_config = {"bootstrap.servers": args.bootstrap_servers}
    producer = Producer(**producer_config)

    # Schema Related
    schema_registry_conf = {"url": args.schema_registry}
    schema_regirstry_client = SchemaRegistryClient(schema_registry_conf)

    schema_str = read_schema(args.schema_path, output_type="str")

    # Serializer
    string_serializer = StringSerializer("utf_8")
    
    json_serializer = JSONSerializer(
        schema_str=schema_str, 
        schema_registry_client=schema_regirstry_client,
        # to_dict=model_to_dict
    )

    # read data file
    with open(args.source_path, "r") as f:
        json_lines = [record for record, _ in BaseLoader().read_json_lines_file(file=f, rows=args.rows)]

    print(f"Number of processing row: {len(json_lines)}")
    # loop to send data
    while True:
        producer.poll(args.streaming_interval)
        try:
            for record in json_lines:
                producer.produce(
                    topic       = args.topic,
                    key         = string_serializer(str(record.get("id"))),
                    # value       = str(record),
                    value       = json_serializer(record, SerializationContext(args.topic, MessageField.VALUE)),
                    on_delivery = ack
                )
        except KeyboardInterrupt as err:
            print(f"{err}: Manually Terminated Kafka Session.")
            print("\nFlushing records")
            producer.flush()
            break
        except Exception as err:
            raise f"Unexpected Error: {err}"

    print("\nFlushing records")
    producer.flush()

if __name__ == '__main__':
    parser = argparse.ArgumentParser("Confluent Kafka Producer")
    
    parser.add_argument(
        "--bootstrap-servers", required=True, type=str,
        help="Bootstap broker(s) (host[:port]) e.g. localhost:xxxx,localhost:yyyy"
    )
    
    parser.add_argument(
        "--schema-registry", required=True, type=str, 
        help="Schema Registry e.g. http(s)://host[:port]"
    )
    
    parser.add_argument(
        "--topic", required=True, type=str, 
        help="my-topic-name"
    )

    parser.add_argument(
        "--source-path", required=True, type=str,
        help="Relative path to datasource e.g. './path/to/datasource/file.json'"
    )

    parser.add_argument(
        "--schema-path", required=True, type=str,
        help="Relative path to schema for schema registry e.g. './path/to/schema/file.json'"
    )

    parser.add_argument(
        "--rows", required=True, type=int,
        help="number of row from datasource to load to a topic"
    )

    parser.add_argument(
        "--streaming-interval", required=False, type=float, default=0.0,
        help="1"
    )

    main(parser.parse_args())
