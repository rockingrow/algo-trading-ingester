.PHONY: help install install-dev update lock fix format lint check test run

help:
	@echo "Available commands:"
	@echo "  make install       - Install production dependencies"
	@echo "  make install-dev   - Install all dependencies including dev"
	@echo "  make update        - Upgrade dependencies and regenerate uv.lock"
	@echo "  make lock          - Regenerate uv.lock"
	@echo "  make fix           - ruff format + ruff check --fix"
	@echo "  make format        - ruff format"
	@echo "  make lint          - ruff check"
	@echo "  make test          - Run the pytest suite"
	@echo "  make run           - Run the ingester (reads .env)"

install:
	uv sync --no-dev

install-dev:
	uv sync

update:
	uv lock --upgrade
	uv sync

lock:
	uv lock

fix:
	uv run ruff format .
	uv run ruff check --fix .

format:
	uv run ruff format .

lint:
	uv run ruff check .

check: lint

test:
	uv run pytest

start:
	uv run python -m ingester
