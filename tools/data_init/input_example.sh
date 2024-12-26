# convert data to json lines
python tools/data_init/convertor_main.py

# mongodb example
# data_path
python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination mongodb \
    --mongo-db kde-db \
    --mongo-collection kde-finance-cards-data \
    --data-path data/json/cards_data.json \
    --rows 10

python ./tools/data_init/loader_main.py \
    --load-type streaming \
    --destination mongodb \
    --mongo-db kde-db \
    --mongo-collection kde-finance-cards-data \
    --data-path data/json/cards_data.json \
    --rows 20 \
    --streaming-interval 0.5

# random_api
python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination mongodb \
    --mongo-db kde-db \
    --mongo-collection kde-finance-random-user \
    --random-api \
    --rows 20

python ./tools/data_init/loader_main.py \
    --load-type streaming \
    --destination mongodb \
    --mongo-db kde-db \
    --mongo-collection kde-finance-random-user \
    --random-api \
    --rows 30 \
    --streaming-interval 0.5

