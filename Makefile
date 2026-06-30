# Makefile for AnomalyGate

.PHONY: setup generate train run run-once feed consume clean test lint

setup:
	pip install -r requirements.txt
	docker compose up -d

generate:
	python main.py generate 10000

train:
	python main.py train

run:
	python main.py run

run-once:
	python main.py run --once

feed:
	docker compose exec -T kafka kafka-console-producer --bootstrap-server localhost:9092 --topic raw-security-logs < sample_logs.json

consume:
	docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic siem-critical-logs --from-beginning --max-messages 10

test:
	python -m pytest tests/ -v

lint:
	flake8 src/ tests/
	black --check src/ tests/

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
	rm -rf checkpoints
	rm -f sample_logs.json
