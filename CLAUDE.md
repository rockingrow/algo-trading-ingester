@AGENTS.md

# Claude Code

The line above imports [AGENTS.md](AGENTS.md), the shared instruction file every
coding agent in this repository follows — project layout, the navigation table,
architecture invariants, the rules and verification. Claude Code does not read
`AGENTS.md` on its own; that import is what puts it in context.

**Do not copy shared content into this file.** Anything both Codex and Claude
need goes in `AGENTS.md`; this file holds only what is specific to Claude Code.
Two files describing the same repository is how they start contradicting each
other.

## Before handing work back

- `/code-review` on the diff before the branch is proposed for a PR into `dev`.
- `/security-review` when the change touches `.env` or `config/*.toml` handling,
  MT5 credentials, Telegram tokens, or anything published to NATS.
- Push to the working branch and report the commands you ran, including the
  test baseline comparison AGENTS.md asks for.

## Where to slow down

Use plan mode, and confirm the approach, before editing:

- `ingester/schemas/` — the wire contract `qte-ingest` decodes; a change here
  is a change for a consumer this repository cannot see.
- `ingester/gateways/forex/mt5/{ingestion,dto}.py` — bar-close detection and the
  server-time → UTC conversion; a mistake here silently drops, duplicates or
  time-shifts the bars strategies trade on.
- `ingester/gateways/crypto/binance/{ingestion,dto}.py` — the same risk on the crypto
  side: the `k.x` close flag, the per-stream de-duplication and the
  millisecond → UTC conversion.
- `ingester/core/ingestion.py` — every gateway inherits it, and it owns the
  thread → event-loop hand-off.

## Session hygiene

- Durable project facts belong in `AGENTS.md`, not in auto memory: Codex has to
  see them too.
- If `/context` does not list both `CLAUDE.md` and `AGENTS.md` under **Memory
  files**, the import broke — check that the first line of this file is
  `@AGENTS.md` outside any code fence.
- The `.claude/hooks/session-start.sh` hook runs `uv sync --group dev` in
  remote sessions only; locally, run `make install-dev` yourself before
  expecting `ruff`/`pytest` to exist.
