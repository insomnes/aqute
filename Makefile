.PHONY: install lint ruff-check-format ruff ty format test check build

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

check: lint test

build:
	uv build --no-sources
