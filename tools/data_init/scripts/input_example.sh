# Load Data from Local / Random API to Different Target


# MongoDB Example

# Require Manual creating database on MongoDB Atlas via WebUI

# Initiate MongoDB Database and Collections
python tools/db_init/mongo_init.py --database kde-db --prefix kde

# Clean Colelctions
# python tools/db_init/mongo_init.py --database kde-db --reset

# Load with Local Data (data_path)

python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination mongodb \
    --database kde-db \
    --collection kde-finance-cards-data \
    --data-path data/json/cards_data.json \
    --rows 10

python ./tools/data_init/loader_main.py \
    --load-type streaming \
    --destination mongodb \
    --database kde-db \
    --collection kde-finance-cards-data \
    --data-path data/json/cards_data.json \
    --rows 20 \
    --streaming-interval 0.5

# Load Data from random_api

python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination mongodb \
    --database kde-db \
    --collection kde-finance-random-user \
    --random-api \
    --rows 20

python ./tools/data_init/loader_main.py \
    --load-type streaming \
    --destination mongodb \
    --database kde-db \
    --collection kde-finance-random-user \
    --random-api \
    --rows 10 \
    --streaming-interval 0.5


# Firestore Example

# Require Terraform to create Database on GCP first

# Firestore don't require to initiate Collections

# Clean Collections
python tools/db_init/firestore_init.py --database '(default)' --reset
# python tools/db_init/firestore_init.py --database '(default)' --prefix kde --reset

# Load with Local Data (data_path)

python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination firestore \
    --project-name ${GCP__PROJECT} \
    --database '(default)' \
    --collection kde-finance-cards-data \
    --data-path data/json/cards_data.json \
    --rows 20

python ./tools/data_init/loader_main.py \
    --load-type streaming \
    --destination firestore \
    --project-name ${GCP__PROJECT} \
    --database '(default)' \
    --collection kde-finance-cards-data \
    --data-path data/json/cards_data.json \
    --rows 10 \
    --streaming-interval 0.5

# Load Data from random_api

python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination firestore \
    --project-name ${GCP__PROJECT} \
    --database '(default)' \
    --collection kde-finance-random-user \
    --random-api \
    --rows 20

python ./tools/data_init/loader_main.py \
    --load-type streaming \
    --destination firestore \
    --project-name ${GCP__PROJECT} \
    --database '(default)' \
    --collection kde-finance-random-user \
    --random-api \
    --rows 10 \
    --streaming-interval 0.5


# DynamoDB Example

# Require Terraform to create DynamoDB resources on AWS first

# Resetting can be done by destroying DynamoDB resources via Terraform

# Load with Local Data (data_path)

python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination dynamodb \
    --profile kde-local-sso-cli \
    --table cards-data \
    --data-path data/json/cards_data.json \
    --rows 5

python ./tools/data_init/loader_main.py \
    --load-type streaming \
    --destination dynamodb \
    --profile kde-local-sso-cli \
    --table cards-data \
    --data-path data/json/cards_data.json \
    --rows 10 \
    --streaming-interval 0.5

# Load Data from random_api

# python ./tools/data_init/loader_main.py \
#     --load-type batch \
#     --destination dynamodb \
#     --profile kde-local-sso-cli \
#     --table cards-data \
#     --random-api \
#     --rows 15

# python ./tools/data_init/loader_main.py \
#     --load-type streaming \
#     --destination dynamodb \
#     --profile kde-local-sso-cli \
#     --table cards-data \
#     --random-api \
#     --rows 20 \
#     --streaming-interval 0.5


# Kafka Example

# Require to start Kafka containers

# Resetting can be done by deleting Kafka Containers

# Load with Local Data (data_path)

python ./tools/data_init/loader_main.py \
    --load-type streaming \
    --destination kafka \
    --bootstrap-server localhost:9092 \
    --topic cards_data \
    --data-path data/json/cards_data.json \
    --rows 10 \
    --streaming-interval 0.5

# Load Data from random_api

python ./tools/data_init/loader_main.py \
    --load-type streaming \
    --destination kafka \
    --bootstrap-server localhost:9092 \
    --topic random-api \
    --random-api \
    --rows 10 \
    --streaming-interval 0.5
