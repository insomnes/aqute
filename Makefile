BC="\\033[1\;96m"
YC="\\033[1\;33m"
NC="\\033[39m"

.PHONY: install lint black ruff isort mypy black-format isort-format format test

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
