.PHONY: help install install-dev update lock fix format lint check test start status stop forex crypto

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
	@echo "  make start         - Run the ingester in the foreground, Ctrl-C to stop (reads .env)"
	@echo "  make status        - Report whether the ingester is running and what each gateway is doing"
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

# The only way this repository runs the service: in the foreground, logs on the
# console, Ctrl-C stopping it in order (gateways -> NATS drain -> Telegram).
# Running it in the background is the host's job - systemd or pm2 on Linux, a
# service wrapper or Task Scheduler on Windows - because a supervisor restarts
# it on failure, starts it at boot and rotates what it captures, and because an
# "ExecStop" sending SIGINT gets that ordered shutdown where a hard kill cannot.
# Set LOG_CONSOLE=false there: the supervisor already captures stdout, and the
# mirror costs a synchronous write on the thread that logged.
start:
	uv run python -m ingester

# Read-only check: who holds APP_PORT, then what GET /status answers. Exits
# non-zero when the service is down or degraded, so make prints its own
# "Error 1" line under the report — that is the answer, not a broken target.
# PORT=... / HOST=... override .env for a one-off.
status:
	uv run python -m ingester.status $(if $(PORT),--port $(PORT),) $(if $(HOST),--host $(HOST),)

# Force-stop the ingester: kills every process bound to APP_PORT from .env, so
# a crashed or wedged run cannot keep the port and block the next "make start".
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
