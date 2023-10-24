BC="\\033[1\;96m"
YC="\\033[1\;33m"
NC="\\033[39m"

.PHONY: install lint black ruff isort mypy black-format isort-format format test mypy-strict

install:
	@poetry install --with test,dev

ruff:
	@echo -e "${BC}Linting via ruff${NC}" && ruff .

isort:
	@echo -e "${BC}Linting via isort${NC}" && isort --check .

black:
	@echo -e "${BC}Linting via black${NC}" && black --check .

mypy:
	@echo -e "${BC}Linting via mypy${NC}" && mypy .

lint: ruff isort black mypy


isort-format:
	@echo -e "${YC}Formatting via isort${NC}" && isort .

black-format:
	@echo -e "${YC}Formatting via black${NC}" && black .

format: isort-format black-format

test:
	@pytest -v .

mypy-strict:
	@mypy \
		--disallow-any-unimported \
		--disallow-any-expr \
		--disallow-any-decorated \
		--disallow-any-explicit \
		--disallow-any-generics \
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
