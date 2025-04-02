# kafka schema registry
# delete registered schema from schema registry
curl -X DELETE http://localhost:8081/subjects/cards-data-value

# kafka connect

# deploy kafka connector
curl -X POST -H "Content-Type: application/json" --data @connectors_config/s3_sink_cards_data.json http://localhost:8083/connectors

# force check connector's status
curl -s http://localhost:8083/connectors/s3-sink-kde/status

# delete deployed kafka connector
curl -X DELETE http://localhost:8083/connectors/s3-sink-kde