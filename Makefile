# Makefile for AnomalyGate

.PHONY: setup generate train run clean test lint

setup:
	pip install -r requirements.txt
	docker compose up -d

generate:
	python main.py generate 10000

train:
	python main.py train

run:
	python main.py run

test:
	pytest tests/ -v

lint:
	flake8 src/ tests/
	black --check src/ tests/

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
	rm -f sample_logs.json
