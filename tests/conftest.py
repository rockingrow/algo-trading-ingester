"""
tests/conftest.py — Keep a test run out of the operator's files.

``ingester.logger`` resolves ``LOG_DIR`` **at import time**, from the real
``.env``, so without this the suite appends its fakes to the day's service log:
the operator's log then shows EURUSD bars that no venue sent, a watchdog armed
after one attempt and a Telegram that is "down", mixed into the lines a running
ingester wrote. The log is what an incident is reconstructed from, so nothing
but the service may write to it.

The variables are set before any ``ingester`` module is imported — a conftest
is loaded ahead of the test modules — and an environment variable wins over
``.env`` in pydantic-settings, so this holds whatever the operator configured.
``SOURCE_CONFIG_DIR`` goes with them: a test that loads a market must not find,
or depend on, the symbols this host happens to ingest.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

#: One directory per run, outside the repository. Left behind on purpose: a
#: failing run's log is evidence, and the OS reclaims the temp directory.
_TEST_LOG_DIR = Path(tempfile.mkdtemp(prefix="ingester-tests-"))

os.environ["LOG_DIR"] = str(_TEST_LOG_DIR)
os.environ.setdefault("SOURCE_CONFIG_DIR", str(_TEST_LOG_DIR / "config"))
