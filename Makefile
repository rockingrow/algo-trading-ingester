.PHONY: help install install-dev update lock fix format lint check test run dev start stop forex crypto

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
	@echo "  make dev           - Run the ingester in the foreground, Ctrl-C to stop (reads .env)"
	@echo "  make start         - Start the ingester detached, in the background"
	@echo "  make stop          - Force-stop every process holding APP_PORT (.env)"
	@echo "  make forex         - Create config/forex.toml from its example, comments stripped (keeps an existing file)"
	@echo "  make crypto        - Create config/crypto.toml from its example, comments stripped (keeps an existing file)"

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

# Blocking foreground run: logs on the console, Ctrl-C stops it in order
# (gateways -> NATS drain -> Telegram). "run" is kept as its older name.
dev:
	uv run python -m ingester

run: dev

# Detached background run: returns the shell at once and survives it. Refuses
# to start when APP_PORT is already held, and appends whatever the process
# prints to LOG_DIR/ingester.out.log. Stop it with "make stop".
start:
	uv run python -m ingester.start $(if $(PORT),--port $(PORT),)

# Force-stop the ingester: kills every process bound to APP_PORT from .env, so
# a detached or wedged run cannot keep the port and block the next "make run".
# PORT=... overrides it for a one-off.
stop:
	uv run python -m ingester.stop $(if $(PORT),--port $(PORT),)

# Never overwrite: config/<market>.toml is the operator's own, git-ignored file.
# The rule has no prerequisite, so make only runs it when the file is missing.
# The copy keeps the template's header block (up to its first blank line) and
# drops every comment-only and blank line after it, so the operator's file is
# just the tables and their values. Done through Python so it works under
# cmd.exe as well as sh, and creates nothing when the template is not there.
forex: config/forex.toml
crypto: config/crypto.toml

config/%.toml:
	@uv run python -c "L=open('config/$*.example.toml',encoding='utf-8').read().splitlines();i=L.index('');open('$@','w',encoding='utf-8').write('\n'.join(L[:i+1]+[x for x in L[i+1:] if x.strip() and not x.lstrip().startswith('#')])+'\n')"
	@echo Created $@
