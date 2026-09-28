#!/bin/bash
#
# SessionStart hook — installs project dependencies so tests and linters
# work in Claude Code on the web sessions.
#
# Runs synchronously: the session waits until dependencies are installed,
# guaranteeing tooling (ruff, pytest) is ready before the agent acts.
#
set -euo pipefail

# Only run in the remote (Claude Code on the web) environment.
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

cd "${CLAUDE_PROJECT_DIR:-.}"

# Install all dependencies, including the dev group (ruff, pytest,
# pytest-asyncio). `uv sync` is idempotent and benefits from container
# state caching across sessions.
uv sync --group dev
