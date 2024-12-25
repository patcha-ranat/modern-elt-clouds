# Modern ELT Pipeline with Clouds

*Patcharanat P.*

## Getting Started
```bash
# .env
# TODO: Get MongoDB Connection URI From Web UI
MONGO_URI="XXX"
```

## Setting up Environment

```bash
make venv

source pyenv/Scripts/activate

make install

./tools/data_init/kaggle_wrapper.sh

python tools/data_init/convertor_main.py

python tools/db_init/mongo_init.py # kde-db # kde

python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination mongodb \
    --mongo-db kde-db \
    --mongo-collection kde_finance_cards_data \
    --data-path data/json/cards_data.json \
    --rows 10
```

*In progress . . .*