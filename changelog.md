# Changelog

## Unreleased

### Breaking — schema `2.0`

- Renamed `source.ingestor_id` to `source.ingester_id` in every published
  event. `qte-ingest` must read the new field name.
- `SCHEMA_VERSION` bumped from `1.0` to `2.0`.

### Breaking — configuration

- **Several markets in one process.** `SOURCE_MARKET` in `.env` names the
  markets to run (`forex,crypto`); each is read from
  `<SOURCE_CONFIG_DIR>/<market>.toml` and started side by side. Replaces
  `APP_GATEWAYS`, which is **gone**.
- **Gateway settings moved from `.env` to `config/<market>.toml`.** One
  `[gateway]` table per venue, `enable = true/false` first, keys lower case.
  A gateway's market is the file its table lives in, so `MT5_MARKET` is
  **gone** too. What moved out of `.env`:

  | `.env` (removed) | `config/forex.toml` |
  | --- | --- |
  | `MT5_SYMBOLS` | `[mt5] symbols` |
  | `MT5_SYMBOL_SUFFIX` | `[mt5] symbol_suffix` |
  | `MT5_TIMEFRAMES` | `[mt5] timeframes` |
  | `MT5_SERVER_TIMEZONE` | `[mt5] server_timezone` |
  | `MT5_POLL_INTERVAL_SECONDS` | `[mt5] poll_interval_seconds` |
  | `MT5_CATCHUP_BARS` | `[mt5] catchup_bars` |
  | `MT5_BACKFILL_ON_START` | `[mt5] backfill_on_start` |
  | `MT5_RECONNECT_INTERVAL_SECONDS` | `[mt5] reconnect_interval_seconds` |

  `MT5_TERMINAL_PATH`, `MT5_LOGIN`, `MT5_PASSWORD`, `MT5_SERVER` and
  `MT5_TIMEOUT_MS` **stay in `.env`**: they are secrets and host paths, and the
  market files are meant to be readable and diffable.
- `config/*.toml` is git-ignored; `config/forex.example.toml` and
  `config/crypto.example.toml` are the committed templates. Copy them after
  pulling: a market with no file is reported as **Ingester Degraded**.

The published payload is unchanged — `SCHEMA_VERSION` stays `2.0` and
`qte-ingest` needs no change for this.

### Added

- **Binance gateway (`market = crypto`).** Closed bars from the public kline
  websocket: one combined stream carries every symbol × timeframe, and only the
  update Binance flags `k.x == true` is published. Canonical `close_time` is
  `open_time + timeframe` rather than Binance's `T` (one millisecond short of
  the next open), `quote_volume` and the trade count are carried, and `spread`
  is `null` — an order book quotes no broker spread. Per-stream de-duplication
  means a reconnect that replays a bar publishes it once. Runs on any OS and
  needs no API key. Example payload:
  `examples/nats/bar.closed.binance.json`.
- **Markets fail independently.** A market whose file is missing, unparseable,
  names an unknown gateway or mistypes a key is reported as **Ingester
  Degraded**; the markets that loaded still ingest. The same holds for a
  gateway that will not start — MT5 on a non-Windows host no longer takes the
  crypto market down with it. A process where nothing ends up ingesting (every
  `enable = false`, or an empty `SOURCE_MARKET`) is degraded too, rather than
  reporting itself healthy while publishing nothing.
- **Market files cannot overreach.** A table setting `market` is refused — it
  could only contradict the file it lives in — and so are `login`, `password`,
  `server`, `terminal_path` and `timeout_ms`, which stay in `.env`. One gateway
  enabled in two markets is refused in the second: MetaTrader 5 keeps a single
  process-wide terminal session, and `event_id` carries no market, so two
  instances would publish colliding ids.
- `/status` and the **Ingester Running** notification now name each gateway's
  market, so `mt5` and `binance` streams are traceable to their file.
- `websockets` is now a direct dependency (it was already present through
  `uvicorn[standard]`); the Binance gateway imports it.

### Added (MT5)

- **Broker affix detection.** `[mt5].symbols` names the bare instrument
  (`XAUUSD`); the gateway asks the terminal what this broker calls it
  (`XAUUSDm` on Exness) and reads bars from that name. The published `symbol`
  and therefore `event_id` stay the configured name, so the wire contract is
  unchanged and survives a change of broker. `symbol_suffix` settles a tie
  between equally close candidates; a fully spelled name is used verbatim.
- **`[mt5].backfill_on_start`.** Publishes the closed bars found at start-up
  instead of only recording them, so a crash no longer silently skips the bars
  that closed while the process was down. Bounded by `catchup_bars`; pair it
  with JetStream, which drops the replayed duplicates by `event_id`.
- **Errors in their own Telegram chat.** `TELEGRAM_LOG_ERRORS_ENABLED` mirrors
  every `ERROR` record to `TELEGRAM_LOG_CHAT_IDS` with an optional separate
  bot token, its own queue and a `TELEGRAM_LOG_DEDUP_WINDOW` suppression
  window.

### Changed

- **Start-up failures no longer stop the process.** An unreachable NATS or a
  gateway that will not start is logged, reported as **Ingester Degraded** and
  survived; NATS reconnects underneath. A supervisor restart would only drop
  more bars.
- One malformed record on one symbol is logged and skipped instead of aborting
  the whole poll cycle, so it can no longer starve the other symbols.
- JetStream: an existing stream whose subjects miss `NATS_SUBJECT_PREFIX`, or
  whose duplicate window is shorter than configured, is now reported. Streams
  are still never reconfigured.
- The **Ingester Failed To Start** notification is gone: there is no longer a
  start-up path that reports it. **Ingester Degraded** replaces it.
- Spelling harmonised repository-wide: `ingestor` → `ingester`. The Python
  package is now `ingester/` (`python -m ingester`), the project is
  `algo-trading-ingester`, and `APP_NAME` defaults to `algo-trading-ingester`.
  No environment variable name changed.
