.PHONY: config up down clean register status workload topic check verify

config:
	docker compose config --quiet

up:
	docker compose up -d --wait

down:
	docker compose down

clean:
	docker compose down --volumes --remove-orphans

register:
	python3 scripts/register_connector.py

status:
	curl -s http://localhost:18083/connectors/orders-cdc/status; echo

workload:
	python3 scripts/workload.py --transactions 200 | \
		docker compose exec -T postgres psql -U app -d app -v ON_ERROR_STOP=1 -q

topic:
	docker compose exec redpanda rpk topic consume cdc.app.orders --num 5

check:
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -v
	docker compose config --quiet

verify:
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/verify_runtime.py
