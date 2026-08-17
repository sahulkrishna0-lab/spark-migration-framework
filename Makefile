.PHONY: test lint format typecheck clean install all

# Default target
all: lint typecheck test

# Install dependencies
install:
	pip install -r requirements.txt

# Run tests
test:
	pytest tests/ -v --tb=short

# Run tests with coverage
coverage:
	pytest tests/ -v --cov=src --cov-report=html --cov-report=term-missing

# Lint with ruff
lint:
	ruff check src/ tests/

# Auto-format code
format:
	ruff format src/ tests/

# Type checking
typecheck:
	mypy src/ --ignore-missing-imports

# Clean artifacts
clean:
	rm -rf __pycache__ .pytest_cache .mypy_cache htmlcov .coverage
	rm -rf dags/*.py
	find . -type d -name __pycache__ -exec rm -rf {} +

# Generate DAG from sample config
generate-dag:
	python -m src.dag_generator --config configs/sample_pipeline.yaml --output dags/

# Run schema validation
validate:
	python -m src.schema_validator --config configs/sample_pipeline.yaml
