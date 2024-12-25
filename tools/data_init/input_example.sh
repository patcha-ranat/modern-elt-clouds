# convert data to json lines
python tools/data_init/convertor_main.py

# mongodb example
python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination mongodb \
    --mongo-db kde-db \
    --mongo-collection kde_finance_cards_data \
    --data-path data/json/cards_data.json \
    --rows 10

python ./tools/data_init/loader_main.py \
    --load-type streaming \
    --destination mongodb \
    --mongo-db kde-db \
    --mongo-collection kde_finance_cards_data \
    --data-path data/json/cards_data.json \
    --rows 20 \
    --streaming-interval 0.5
