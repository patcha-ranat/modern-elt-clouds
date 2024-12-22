.SILENT: venv install

GREEN=\033[0;32m
YELLOW=\033[0;33m
GREY=\033[0;30m
NC=\033[0m # No Color

venv:
	python -m venv pyenv
	echo -e "${YELLOW}Please, execute 'source pyenv/Scripts/activate' before 'make install'${NC}"

install:
	pip install -r .airflow/requirements.txt
	pip install -r tools/requirements.txt

start:
	docker compose -f .docker/docker-compose.yml up --build

stop:
	docker compose -f .docker/docker-compose.yml down -v
