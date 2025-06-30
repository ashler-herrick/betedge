# Makefile for betedge_data testing

.PHONY: help test test-unit test-integration test-e2e test-all test-coverage bench clean install

help:
	@echo "Available commands:"
	@echo "  make install        - Install dependencies"
	@echo "  make test          - Run unit tests only"
	@echo "  make test-unit     - Run unit tests only"
	@echo "  make test-integration - Run integration tests"
	@echo "  make test-e2e      - Run end-to-end tests"
	@echo "  make test-all      - Run all tests"
	@echo "  make test-coverage - Run tests with coverage report"
	@echo "  make bench         - Run performance benchmarks"
	@echo "  make clean         - Clean up generated files"
	@echo "  make format        - Format code"
	@echo "  make lint          - Lint all code"

install:
	uv sync

test: uv run pytest

test-unit:
	uv run pytest tests/unit -v -m unit

test-integration:
	uv run pytest tests/integration -v -m integration

test-e2e:
	uv run pytest tests/e2e -v -m e2e

test-all:
	uv run pytest tests -v

test-coverage:
	uv run pytest tests/unit --cov=betedge_data --cov-report=html --cov-report=term

test-coverage-all:
	uv run pytest tests --cov=betedge_data --cov-report=html --cov-report=term

bench:
	uv run pytest bench -v

test-watch:
	uv run pytest tests/unit -v -m unit --watch

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name ".coverage" -delete

# Run specific test file
test-file:
	@if [ -z "$(FILE)" ]; then \
		echo "Usage: make test-file FILE=path/to/test_file.py"; \
	else \
		uv run pytest $(FILE) -v; \
	fi

# Run tests matching a pattern
test-match:
	@if [ -z "$(MATCH)" ]; then \
		echo "Usage: make test-match MATCH=pattern"; \
	else \
		uv run pytest tests -v -k $(MATCH); \
	fi

format:
	uvx ruff format

lint:
	uvx ruff check --fix