# Repository Instructions

Shared instructions for every coding agent working in this repository. Codex
reads this file directly; Claude Code reads it through the `@AGENTS.md` import
at the top of `CLAUDE.md`. **Keep shared content here** — anything specific to
one tool goes in that tool's own file, so the two never drift apart.

Follow a more specific `AGENTS.md` in a subdirectory when one exists.

## The project in five lines

Market-data gateway. Each gateway (MetaTrader 5 today, Binance websocket next)
detects **closed bars**, turns the venue payload into the canonical
`BarClosedEvent` through its DTO, and publishes it **one way** to NATS on
`<NATS_SUBJECT_PREFIX>.bar.closed.<gateway>.<symbol>.<timeframe>`, where
[Quant-Trading-Engine](https://github.com/rockingrow/quant-trading-engine)'s
`qte-ingest` consumes it. Service status goes to Telegram. Python 3.13, uv,
FastAPI, NATS; the `MetaTrader5` package exists for Windows only.

## Rules

1. **Commits**: only when the user asks. English, imperative, 72-char subject
   at most. **No AI attribution of any kind** — no `Generated with Claude
   Code`, no `Co-Authored-By: Claude`, no ChatGPT/Codex footer, no session link.
2. **English** for comments, docstrings, identifiers, log and exception strings,
   and every committed Markdown file.
3. **Names**: explicit and domain-flavoured (`timeframe`, `open_time`, not
   `tf`/`ot` in new public code). Exceptions for names fixed by an external
   contract (MT5 `tick_volume`, Binance kline fields) and whole-word market
   terms. Do not rename legacy variables the task does not touch.
4. **Pull requests always target `dev`** — never `master`/`main`. Opening the
   PR is the user's call.
5. **Payload changes are contract changes.** A field added to or changed in
   `ingester/schemas/` is a promise to `qte-ingest`: update
   `examples/nats/`, the README "NATS contract" section and `changelog.md` in
   the same change, and bump `SCHEMA_VERSION` on anything breaking.
6. **All configuration comes from `.env`.** Symbols, timeframes, gateways,
   host/port, NATS and Telegram settings are never hard-coded; a new setting
   goes into `ingester/settings.py` *and* `.env.example` with a comment.
7. **Never** expose, commit or copy secrets from `.env`, tokens, chat ids, MT5
   logins/passwords or account identifiers. `.env.example` carries
   placeholders only.

## Working approach

- Read the relevant source, tests, configuration and documentation before
  editing.
- Inspect `git status` first. Preserve every unrelated change and untracked
  file in the working tree.
- Make the smallest coherent change that solves the problem and matches the
  existing architecture.
- Do not add or upgrade production dependencies unless the task requires it;
  say why when you do.

## Commands

```bash
make install-dev    # uv sync (dev group: ruff, pytest, pytest-asyncio)
make lint / format  # ruff check . / ruff format .   (make fix = both, with --fix)
make test           # uv run pytest
make run            # uv run python -m ingester   (reads .env)
make help           # every target, one line each

uv run pytest -q                            # whole suite (MT5 is faked)
uv run pytest tests/test_mt5_ingestion.py -q # one file while iterating
```

The MT5 gateway only starts on Windows with a running terminal; on any other OS
the app stops at start-up with a clear `MetaTrader5 package is not installed`
error, which is expected.

## Repository navigation

Route the task with the table before searching. Start inside the owning
package; never scan from the repository root.

1. Table below, to find the owning module.
2. `sed -n '1,25p' <file>` — modules open with a docstring stating their job
   and their trade-offs.
3. `rg -n "<symbol>" ingester tests` — scope the search.
4. `tests/test_<topic>.py` — the suite is organised by topic and reads as the
   executable spec for that module.

| Task or concept | Primary location |
| --- | --- |
| Canonical wire contract (`Bar`, `BarClosedEvent`, `event_id`, subjects) | `ingester/schemas/market_event_schema.py` |
| Shared enums (`Timeframe`, `GatewayEnum`, `MarketEnum`, statuses) | `ingester/schemas/enums.py` |
| Ingestion lifecycle, hand-off queue, dispatcher, status + notifications | `ingester/core/ingestion.py` (`BaseIngestion`, `ThreadedIngestion`) |
| Gateway registry / factory | `ingester/core/factory.py`, registration in `ingester/providers.py` |
| Interfaces (publisher, notifier, ingestion, DTO) | `ingester/interfaces/` |
| MT5 bar-close detection (business logic) | `ingester/gateways/mt5/ingestion.py` |
| MT5 rate → canonical `Bar`, server time → UTC | `ingester/gateways/mt5/dto.py` |
| MetaTrader5 package adapter | `ingester/gateways/mt5/terminal.py` |
| NATS connection and one-way publisher | `ingester/services/nats_service.py` |
| Telegram notifier, queue decorator | `ingester/services/notification_service.py` |
| Telegram message templates, emoji | `ingester/helpers/{messages,emoji_constants}.py` |
| Ordered start/stop of notifier, NATS, gateways | `ingester/runtime.py` |
| FastAPI app, `/health`, `/status` | `ingester/app.py`, `ingester/api/router.py` |
| Settings and environment variables | `ingester/settings.py`, `.env.example` |
| Canonical payload samples | `examples/nats/` |
| How `qte-ingest` consumes market data | [`quant-trading-engine`](https://github.com/rockingrow/quant-trading-engine) `src/qte_shared/` |

Do not scan `.venv/`, `uv.lock`, `__pycache__/`, `.pytest_cache/` or `logs/`.

## Architecture invariants

- **Gateways own business logic only.** A gateway never imports NATS or
  Telegram; the core never imports a venue SDK. A new venue is a
  `GatewaySettings` subclass, a DTO, an ingestion class and one `register` line
  in `providers.py` — nothing else changes.
- **One canonical schema, no per-venue branching downstream.** The DTO is the
  only place that knows a venue's payload shape; past `to_bar()` everything
  speaks `Bar` / `BarClosedEvent`.
- **One way.** The ingester publishes and never subscribes or requests. Status
  goes to Telegram, not to NATS.
- **`event_id` is deterministic** (`<gateway>:<symbol>:<tf>:<open epoch>`). It
  is the subscriber's de-duplication key and the JetStream `Nats-Msg-Id`, so it
  must never include `emitted_at` or anything random.
- **Everything is UTC.** MT5 bar times are trade-server time and are converted
  in the DTO through `MT5_SERVER_TIMEZONE`; naive datetimes are rejected by the
  schema.
- **Thread boundary.** Every MetaTrader5 call runs on the gateway's own thread.
  Crossing into the event loop happens only through `emit_bar` and
  `_set_status`, which are the thread-safe entry points.
- **Notifications never block the pipeline.** Everything goes through
  `QueuedNotifier`; a slow Telegram must not delay a bar.

## Code style

- Python 3.13, `uv`. Run Python tooling through `uv run`.
- Ruff: line length **88**, **2-space indent**, double quotes, rules
  `E4,E7,E9,F,I`. Run `make format` before committing.
- Async throughout; pytest runs with `asyncio_mode = "auto"`.
- Every non-trivial module opens with a docstring saying what it does and why.
  Comments explain the trade-off, not the syntax — match the density around
  you.
- Prefer explicit types and domain terminology over clever, compressed code.
- Cover behaviour changes with focused tests, including failure paths and
  boundary cases. Venues are faked in tests (see `tests/fakes.py`).

## Verification

- Run the narrowest relevant tests while iterating.
- Before handing back a code change: `uv run ruff format --check .`,
  `uv run ruff check .` and `uv run pytest -q`.
- The suite is green on `dev`. Before blaming (or excusing) a failure, get the
  baseline on the base branch and compare. Never leave a new failure behind.
- If the NATS payload changed, `tests/test_examples.py` must still pass against
  the updated `examples/nats/` files.
- Report every command you ran and every failure or skipped check. Never claim
  a check passed without running it — in particular, say so when the MT5 path
  was only exercised through the fake terminal.

## Trading and destructive operations

- Do not point the ingester at a live NATS cluster or a live MT5 account, or
  change NATS tokens, Telegram tokens or chat ids from a session without an
  explicit request and confirmation of the target environment.
- A published bar is not recallable: strategies downstream may trade on it.
  Treat any publish to a shared NATS server as a live action unless the
  environment is proven local.
- History rewrites on shared branches are destructive: verify the exact target
  and get explicit approval immediately before running them.
