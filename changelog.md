# Changelog

## Unreleased

### Configuration — the payload schema version is a setting

- `SCHEMA_VERSION` moves out of `market_event_schema.py` into `.env`
  (`SCHEMA_VERSION=1.0.0`, read as `settings.contract.VERSION`). The default is
  unchanged, so the published payload is too. The `SCHEMA_VERSION` constant is
  no longer exported from `ingester.schemas`.

### Configuration — the JetStream stream name is derived, not configured

- `NATS_STREAM_NAME` is gone. The JetStream stream is now named after the first
  token of `NATS_SUBJECT_PREFIX`, because it has to listen on
  `<NATS_SUBJECT_PREFIX>.>` to accept a publish and a stream is never
  reconfigured once created — two separate settings meant one could be changed
  without the other, rejecting every publish long after the edit. Operators
  whose prefix and stream name already matched (the template's `INGEST`) see no
  change; anyone who had them differ keeps publishing under the prefix and a
  stream named after it is created, leaving the old stream's bars behind. Remove
  the line from `.env`; a leftover one is ignored.

### Internal — gateways grouped by market

- Venue packages moved under the market they serve:
  `ingester/gateways/mt5` → `ingester/gateways/forex/mt5` and
  `ingester/gateways/binance` → `ingester/gateways/crypto/binance`. Import
  paths change accordingly and the old ones are gone, and so do the logger
  names printed in the log files and the Telegram error chat
  (`ingester.gateways.mt5.ingestion` → `ingester.gateways.forex.mt5.ingestion`)
  — update any log filter that matches them. Configuration and the published
  payload do not change.
- `IngestionContext.ingestion_dependencies()` hands every gateway builder the
  shared publisher, notifier and instance id, replacing the copies each builder
  carried.

### Internal — shared gateway bases

- `AsyncStreamIngestion` in `core/ingestion.py` owns the connect → receive →
  reconnect loop for asyncio venues; `BinanceIngestion` extends it and keeps
  only its frame handling. Its log lines now name the gateway from
  `GatewayEnum` (`binance connect failed`) instead of `Binance …`.
- Per-stream de-duplication moved into `BaseIngestion` (`_is_new_bar`,
  `_remember_bar`), used by both gateways. The MT5 gateway now remembers each
  bar as it is emitted rather than once per poll, so a record that fails
  mid-window no longer causes the bars before it to be published twice.
- `BaseBarDTO` in `core/dto.py` is the base of `Mt5RateDTO` and
  `BinanceKlineDTO`.

### Removed — `cfd` market

- `MarketEnum.CFD` is gone: `SOURCE_MARKET` accepts `forex` and `crypto` only,
  and `cfd` in it is now refused at start-up. No gateway ever published
  `source.market = "cfd"`, so the payload a subscriber receives is unchanged
  and `SCHEMA_VERSION` stays `1.0.0`; a subscriber that lists the accepted
  market values can drop `cfd`.

### Breaking — schema `1.0.0`

- Renamed `source.ingestor_id` to `source.ingester_id` in every published
  event. `qte-ingest` must read the new field name.

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
  | `MT5_CATCHUP_BARS` | `[mt5] warmup_bars` (renamed) |
  | `MT5_BACKFILL_ON_START` | `[mt5] backfill_on_start` |
  | `MT5_RECONNECT_INTERVAL_SECONDS` | `[mt5] reconnect_interval_seconds` |

  `MT5_TERMINAL_PATH`, `MT5_LOGIN`, `MT5_PASSWORD`, `MT5_SERVER` and
  `MT5_TIMEOUT_MS` **stay in `.env`**: they are secrets and host paths, and the
  market files are meant to be readable and diffable.
- **`catchup_bars` is now `warmup_bars`**, in both `[mt5]` and `[binance]`.
  The old key is refused as an unknown key, so a market file still carrying it
  is reported as **Ingester Degraded** — rename it in your own
  `config/*.toml` after pulling. The published payload is unaffected: it is
  how many closed bars a read covers, never a field on the wire.
- `config/*.toml` is git-ignored; `config/forex.example.toml` and
  `config/crypto.example.toml` are the committed templates. Copy them after
  pulling: a market with no file is reported as **Ingester Degraded**.

The published payload is unchanged — `SCHEMA_VERSION` stays `1.0.0` and
`qte-ingest` needs no change for this.

### Added — the warm-up window is numbered

- **`warmup_index` and `warmup_total`** are two new top-level integers on
  `bar.closed`, set only on a bar whose `warmup_bar` is `true` and `null`
  otherwise. They number the start-up window `1 … warmup_total`, oldest first,
  so `qte-ingest` can end its warm-up on exactly one message — the one where
  `warmup_index == warmup_total` — instead of guessing whether more history is
  still coming. Everything published after it on that subject is live.
- **Order is part of the contract now.** A gateway publishes the whole warm-up
  window before any live bar, oldest first and with no gaps in the index. That
  already held — one publish queue per gateway, drained one event at a time —
  and is now stated in the README and pinned by tests in
  `tests/test_mt5_ingestion.py` and `tests/test_binance_ingestion.py`.
- **`warmup_total` is the window the venue actually returned**, bounded by the
  market file's `warmup_bars` rather than always equal to it. A recently listed
  symbol or a broker with a thin archive hands back fewer bars, and a
  subscriber waiting for `150` of `150` from a broker that only has `143` would
  wait forever. Both gateways also de-duplicate the window by open time before
  numbering it, so the series can never be shorter than the `warmup_total` it
  announces.
- The series is **per `(symbol, timeframe)`**, which is per subject: warm-up
  ends per subject, not once for the gateway.
- **A window is all-or-nothing**: both gateways now convert the whole window
  before publishing the first bar, so a record the schema rejects half-way
  through publishes nothing and the next poll or reconnect retries the window,
  rather than emitting a series that stops short of its own `warmup_total`.
- **A window is not guaranteed to arrive**, so do not block on one forever:
  there is none with `backfill_on_start = false`, none for a stream whose
  history cannot be read, and none when the Binance REST backfill fails. Live
  bars still flow in all three cases. And with core NATS a failed publish is
  logged and counted rather than retried — gate on
  `warmup_index == warmup_total` only with JetStream enabled.
- Neither field is part of `event_id`, for the reason `warmup_bar` is not and
  one more: the numbers describe the window, so the same bar is `150` of `150`
  for one start-up and `1` of `150` for the next.
- Both fields are additive and default to `null`, so `SCHEMA_VERSION` stays
  `1.0.0` and a subscriber that ignores them keeps decoding every message.
  `examples/nats/bar.closed.mt5.warmup.json` is a new sample showing the bar
  that ends a 150-bar window; the two live samples gained
  `"warmup_index": null, "warmup_total": null`.
- With `backfill_on_start = false` there is no warm-up window, so nothing
  carries the numbers. No configuration changed.

### Added — `warmup_bar` on every `bar.closed` payload

- **`warmup_bar`** is a new top-level boolean on `bar.closed`. It is `true` only
  on the bars a process reads back at start-up — the MT5 first poll under
  `backfill_on_start`, and the Binance REST backfill — and `false` on everything
  the live feed delivers, including bars recovered after a reconnect, which are
  late rather than warm-up. `qte-ingest` can warm indicators on a `true` bar
  without acting on it.
- The field is additive and defaults to `false`, so `SCHEMA_VERSION` stays
  `1.0.0` and a subscriber that ignores it keeps decoding every message. A
  payload without the field is a `false`.
- It is deliberately **not** part of `event_id`: the same bar can arrive live
  from one process and as warm-up from the next, and JetStream keeps whichever
  landed first — so de-duplicate on `event_id` as before and read `warmup_bar`
  only to decide whether to act on a bar. Example payloads in `examples/nats/`
  show a live bar (`"warmup_bar": false`).
- JetStream de-duplicates only inside `NATS_DUPLICATE_WINDOW_SECONDS` (default
  120 s), and a warm-up window is usually older than that — 150 bars of M15 is
  37 hours — so those bars do reach the subscriber again. That is exactly where
  the flag earns its keep.

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

### Added (Binance)

- **`[binance].backfill_on_start` and `[binance].warmup_bars`.** The kline
  websocket only carries bars that close while it is connected, so the bars
  that closed before start-up were never published. With
  `backfill_on_start = true` the gateway reads the last `warmup_bars` closed
  bars per stream from Binance's REST klines endpoint — once the socket is
  open, so no bar falls between the two — and publishes them before the live
  ones: a subscriber sees backfill and current bars as one continuous series.
  Only streams nothing has been published for yet are read, so a reconnect
  does not re-read what the socket already delivered, and a REST failure is
  logged without costing the live socket. Both default to off/5, so the
  behaviour of an existing `config/crypto.toml` does not change. Pair it with
  JetStream, which drops the replayed duplicates by `event_id`.
- **`[binance].klines_url` and `[binance].http_timeout_seconds`.** The REST
  endpoint that backfill reads, written out in full because its path differs
  per product (`/api/v3/klines` on spot and the testnet, `/fapi/v1/klines` on
  USD-M futures). Keep it on the same product as `ws_url`.
- `BinanceKlineDTO.from_rest_row()` reads Binance's positional REST rows;
  `KlineHistory` in `gateways/crypto/binance/history.py` is the HTTP seam,
  faked in tests like the websocket.

### Added (MT5)

- **Broker affix detection.** `[mt5].symbols` names the bare instrument
  (`XAUUSD`); the gateway asks the terminal what this broker calls it
  (`XAUUSDm` on Exness) and reads bars from that name. The published `symbol`
  and therefore `event_id` stay the configured name, so the wire contract is
  unchanged and survives a change of broker. `symbol_suffix` settles a tie
  between equally close candidates; a fully spelled name is used verbatim.
- **`[mt5].backfill_on_start`.** Publishes the closed bars found at start-up
  instead of only recording them, so a crash no longer silently skips the bars
  that closed while the process was down. Bounded by `warmup_bars`; pair it
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
