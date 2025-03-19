FROM confluentinc/cp-kafka-connect:7.3.2

RUN confluent-hub install confluentinc/kafka-connect-s3:10.6.0 --no-prompt