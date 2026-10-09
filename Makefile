.PHONY: install test test-integration lint format typecheck openapi migrate-direct migrate-journal \
	image-direct image-journal secrets certs setup \
	compose-up-direct compose-down-direct \
	compose-up-journal compose-down-journal \
	generate-token kind-target submit-work

install:
	uv sync --locked --all-groups

test:
	uv run pytest

test-integration:
	uv run pytest tests/integration -m integration -v

lint:
	uv run ruff check src tests

format:
	uv run ruff format --check src tests

typecheck:
	uv run mypy --strict src

openapi:
	uv run python tools/export_openapi.py

migrate-direct:
	uv run alembic -c alembic.ini upgrade head

migrate-journal: migrate-direct
	uv run alembic -c src/execution_plane_transport_journal/alembic.ini upgrade head

image-direct:
	podman build -f Containerfile.direct -t localhost/execution-plane:dev .

image-journal:
	podman build -f Containerfile.journal -t localhost/execution-plane-journal:dev .

secrets:
	./tools/generate_secrets.sh

certs:
	uv run python tools/generate_certs.py

setup: install secrets certs

compose-up-direct: setup
	uvx podman-compose -p execution-plane-direct -f compose.direct.yaml up --build

compose-down-direct:
	uvx podman-compose -p execution-plane-direct -f compose.direct.yaml down

compose-up-journal: setup
	uvx podman-compose -p execution-plane-journal -f compose.journal.yaml up --build

compose-down-journal:
	uvx podman-compose -p execution-plane-journal -f compose.journal.yaml down

generate-token:
	uv run python tools/generate_jwt_for_ep.py

kind-target:
	uv run python tools/deploy_kind_execution_target.py

submit-work:
	uv run python tools/submit_work_item.py
