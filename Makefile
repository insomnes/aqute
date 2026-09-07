.PHONY: install lint ruff-check-format ruff ty format test docs check build wheel-smoke

SMOKE_PYTHON ?= 3.11

install:
	uv sync --locked

ruff-check-format:
	uv run --locked ruff format --check .

ruff:
	uv run --locked ruff check .

ty:
	uv run --locked ty check

lint: ruff-check-format ruff ty

format:
	uv run --locked ruff check --select I --fix .
	uv run --locked ruff format .

test:
	uv run --locked pytest

docs:
	uv run --locked mkdocs build --strict

check: lint test docs

build:
	uv build --no-sources

wheel-smoke:
	bash scripts/check-wheel.sh "$(SMOKE_PYTHON)"
