# Makefile for AnomalyGate

.PHONY: setup setup-k8s teardown-k8s generate train run run-once feed consume clean test lint

setup:
	pip install -r requirements.txt
	docker compose up -d

setup-k8s:
	pip install -r requirements.txt
	kubectl apply -f k8s-manifest.yaml

teardown-k8s:
	kubectl delete -f k8s-manifest.yaml

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

feed-k8s:
	kubectl exec -i -n anomalygate $$(kubectl get pod -n anomalygate -l app=kafka -o jsonpath='{.items[0].metadata.name}') -- kafka-console-producer --bootstrap-server localhost:9092 --topic raw-security-logs < sample_logs.json

consume:
	docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic siem-critical-logs --from-beginning --max-messages 10

consume-k8s:
	kubectl exec -it -n anomalygate $$(kubectl get pod -n anomalygate -l app=kafka -o jsonpath='{.items[0].metadata.name}') -- kafka-console-consumer --bootstrap-server localhost:9092 --topic siem-critical-logs --from-beginning --max-messages 10

view-flaggings:
	python main.py flaggings

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
