BC="\\033[1\;96m"
YC="\\033[1\;33m"
NC="\\033[39m"

.PHONY: install lint black ruff isort mypy black-format isort-format format test mypy-strict

install:
	@poetry install --with test,lint

ruff-check-format:
	@echo -e "${BC}Checking ruff format${NC}" && ruff format --check .

ruff:
	@echo -e "${BC}Checking ruff rules${NC}" && ruff check .

mypy:
	@echo -e "${BC}Linting via mypy${NC}" && mypy .

typos:
	@echo -e "${BC}Linting via typos${NC}" && typos -v aqute tests README.md


lint: ruff-check-format ruff mypy typos


import-format:
	@echo -e "${YC}Formatting imports via ruff${NC}" && ruff check --select=I --fix .

ruff-format:
	@echo -e "${YC}Formatting via ruff${NC}" && ruff format .

format: import-format ruff-format

test:
	@pytest -v .

mypy-strict:
	@mypy \
		--disallow-any-unimported \
		--disallow-any-decorated \
		--disallow-subclassing-any \
		--disallow-untyped-calls \
		--disallow-untyped-defs \
		--check-untyped-defs \
		--disallow-untyped-decorators \
		--no-implicit-optional \
		--strict-optional \
		--warn-redundant-casts \
		--warn-unused-ignores \
		--warn-return-any \
		--warn-unreachable \
		--warn-no-return \
		--warn-unused-configs \
		--strict-equality \
		.
