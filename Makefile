.PHONY: setup venv install deps mongo test

setup: venv install

venv:
	python3 -m venv venv
	. venv/bin/activate && pip install --upgrade pip

install:
	. venv/bin/activate && pip install -r requirements.txt

mongo:
	docker-compose up -d

test:
	. venv/bin/activate && pytest -q
