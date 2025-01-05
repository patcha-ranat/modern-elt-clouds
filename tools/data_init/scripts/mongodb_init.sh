#!/bin/bash

# Reset DB, Initialize, and Load converted data to MongoDB using Batch Approach
python tools/db_init/mongo_init.py --database kde-db --reset

python tools/db_init/mongo_init.py --database kde-db --prefix kde

python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination mongodb \
    --db-name kde-db \
    --collection-name kde-finance-random-user \
    --random-api \
    --rows 1000

python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination mongodb \
    --db-name kde-db \
    --collection-name kde-finance-cards-data \
    --data-path data/json/cards_data.json \
    --rows 1000

python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination mongodb \
    --db-name kde-db \
    --collection-name kde-finance-mcc-codes \
    --data-path data/json/mcc_codes.json \
    --rows 1000

python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination mongodb \
    --db-name kde-db \
    --collection-name kde-finance-train-fraud-labels \
    --data-path data/json/train_fraud_labels.json \
    --rows 1000

python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination mongodb \
    --db-name kde-db \
    --collection-name kde-finance-transactions-data \
    --data-path data/json/transactions_data.json \
    --rows 1000

python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination mongodb \
    --db-name kde-db \
    --collection-name kde-finance-users-data \
    --data-path data/json/users_data.json \
    --rows 1000
