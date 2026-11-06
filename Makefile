.PHONY: up down logs test-unit test-integration test scale-consumers demo benchmark

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f api_gateway kafka_consumer

# Unit tests: run in throw-away containers, no Kafka / Postgres needed.
test-unit:
	docker compose run --rm --no-deps api_gateway pytest -m "not integration"
	docker compose run --rm --no-deps kafka_consumer pytest -m "not integration"

# Integration / end-to-end tests: need the stack running (`make up`).
test-integration:
	docker compose exec api_gateway pytest -m integration
	docker compose exec kafka_consumer pytest -m integration

test: test-unit test-integration

scale-consumers:
	docker compose up -d --scale kafka_consumer=3

demo:
	./scripts/demo.sh

benchmark:
	python3 scripts/benchmark.py --requests 50 --concurrency 10
