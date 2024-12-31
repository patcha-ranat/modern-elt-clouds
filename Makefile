.SILENT: venv install

GREEN=\033[0;32m
YELLOW=\033[0;33m
GREY=\033[0;30m
NC=\033[0m # No Color

# Export .env file as environment variables
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

venv:
	python -m venv pyenv
	echo -e "${YELLOW}Please, enable python virtual environment 'pyenv' before 'make install'${NC}"
	echo -e "${GREY}Windows: source pyenv/Scripts/activate${NC}"
	echo -e "${GREY}MacOS: source pyenv/bin/activate${NC}"
install:
	pip install -r .airflow/requirements.txt
	pip install -r tools/requirements.txt

start:
	docker compose -f .docker/docker-compose.yml up --build

stop:
	docker compose -f .docker/docker-compose.yml down -v

test:
	echo "not implemented yet"