# execute dir: ./kafka
python test_producer.py \
    --bootstrap-servers localhost:9092 \
    --schema-registry  http://localhost:8081 \
    --topic cards-data \
    --source-path ../data/json/cards_data.json \
    --schema-path ./schema/json/schema_cards_data.json \
    --rows 50