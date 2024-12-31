# convert data to json lines
python tools/data_init/convertor_main.py

# MongoDB Example

python tools/db_init/mongo_init.py --database kde-db --prefix kde
# python tools/db_init/mongo_init.py --database kde-db --reset

# data_path
python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination mongodb \
    --db-name kde-db \
    --collection-name kde-finance-cards-data \
    --data-path data/json/cards_data.json \
    --rows 10

python ./tools/data_init/loader_main.py \
    --load-type streaming \
    --destination mongodb \
    --db-name kde-db \
    --collection-name kde-finance-cards-data \
    --data-path data/json/cards_data.json \
    --rows 20 \
    --streaming-interval 0.5

# random_api
python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination mongodb \
    --db-name kde-db \
    --collection-name kde-finance-random-user \
    --random-api \
    --rows 20

python ./tools/data_init/loader_main.py \
    --load-type streaming \
    --destination mongodb \
    --db-name kde-db \
    --collection-name kde-finance-random-user \
    --random-api \
    --rows 30 \
    --streaming-interval 0.5

# Firestore Example

# python tools/db_init/firestore_init.py --database '(default)' --prefix kde --reset
python tools/db_init/firestore_init.py --database '(default)' --reset

# data path
python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination firestore \
    --project-name ${GCP__PROJECT} \
    --db-name '(default)' \
    --collection-name kde-finance-cards-data \
    --data-path data/json/cards_data.json \
    --rows 20

python ./tools/data_init/loader_main.py \
    --load-type streaming \
    --destination firestore \
    --project-name ${GCP__PROJECT} \
    --db-name '(default)' \
    --collection-name kde-finance-cards-data \
    --data-path data/json/cards_data.json \
    --rows 10 \
    --streaming-interval 0.5

# random_api
python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination firestore \
    --project-name ${GCP__PROJECT} \
    --db-name '(default)' \
    --collection-name kde-finance-random-user \
    --random-api \
    --rows 20

python ./tools/data_init/loader_main.py \
    --load-type streaming \
    --destination firestore \
    --project-name ${GCP__PROJECT} \
    --db-name '(default)' \
    --collection-name kde-finance-random-user \
    --random-api \
    --rows 20 \
    --streaming-interval 0.5