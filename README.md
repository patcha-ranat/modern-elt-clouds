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

# python tools/db_init/mongo_init.py --database kde-db --reset
python tools/db_init/mongo_init.py --database kde-db --prefix kde

python ./tools/data_init/loader_main.py \
    --load-type batch \
    --destination mongodb \
    --mongo-db kde-db \
    --mongo-collection kde-finance-cards-data \
    --data-path data/json/cards_data.json \
    --rows 10
```

```bash
make start

# make stop
```

References:
- Firestore Python API
    - [Add and update data](https://cloud.google.com/firestore/docs/manage-data/add-data#pythonasync_6)
    - [Delete documents and fields](https://cloud.google.com/firestore/docs/manage-data/delete-data)
- Amazon Authentication
    - [Authenticating using IAM user credentials for the AWS CLI](https://docs.aws.amazon.com/cli/v1/userguide/cli-authentication-user.html)
    - [Root user best practices for your AWS account](https://docs.aws.amazon.com/IAM/latest/UserGuide/root-user-best-practices.html)
- Kafka Docker Compose
    - [Conduktor Kafka Docker Compose Template - GitHub](https://github.com/conduktor/kafka-stack-docker-compose)
*In progress . . .*