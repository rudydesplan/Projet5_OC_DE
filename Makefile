# Makefile for Healthcare MongoDB Loader Project

# ----------------
# Config Variables
# ----------------
PYTHON=python
DOCKER_COMPOSE=docker-compose
DOCKER=docker
CSV_PATH=./app/data/healthcare_dataset.csv
SERVICE_NAME=app

# ----------------
# Setup Commands
# ----------------
install:
	pip install -r app/requirements.txt

lint:
	ruff check app/

format:
	ruff format app/

check: lint test

# ----------------
# Testing Commands
# ----------------
test:
	pytest -v test/test_healthcare_loader.py

test-docker:
	$(DOCKER_COMPOSE) run --rm $(SERVICE_NAME) pytest -v test/test_healthcare_loader.py

# ----------------
# Docker Lifecycle
# ----------------
build:
	$(DOCKER_COMPOSE) build

up:
	$(DOCKER_COMPOSE) up -d

down:
	$(DOCKER_COMPOSE) down

logs:
	$(DOCKER_COMPOSE) logs -f

shell:
	$(DOCKER_COMPOSE) exec $(SERVICE_NAME) sh

# ----------------
# Application Run
# ----------------
run:
	$(DOCKER_COMPOSE) exec $(SERVICE_NAME) python healthcare_mongo_loader_optimized.py

run-csv:
	$(DOCKER_COMPOSE) exec $(SERVICE_NAME) python healthcare_mongo_loader_optimized.py --csv $(CSV_PATH)

# ----------------
# Cleanup
# ----------------
clean:
	find . -type f -name "*.pyc" -delete
	rm -rf __pycache__ .pytest_cache .mypy_cache **/__pycache__ mongo_loader.log

.PHONY: install lint format check test test-docker build up down logs shell run run-csv clean
