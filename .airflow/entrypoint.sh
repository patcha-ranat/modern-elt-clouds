#!/bin/bash

airflow variables import config/variables.json -a overwrite

airflow connections import config/connections.json --overwrite

exec /entrypoint "${@}"