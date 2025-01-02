#!/bin/bash

airflow variables import config/variables.json

airflow connections import config/connections.json --overwrite

exec /entrypoint "${@}"