# Changelog

## Unreleased

### Fixed — an unreachable NATS no longer floods Telegram, and is recovered from

- **The ERROR mirror de-duplicates on the log statement, not on the formatted
  message.** `TELEGRAM_LOG_DEDUP_WINDOW` never collapsed the spam it exists
  for: `"Failed to publish %s: %s"` carries a fresh `event_id` per bar, so a
  NATS that was down turned **every closed bar into its own Telegram
  message**. The key is now `logger:level:<unformatted message>`, and the first
  message that gets through after the window says how many were suppressed
  meanwhile. Two different failures sharing one log statement now read as one
  message plus a count; the full text of each is in the day's log file.
- **A run of identical publish failures is one ERROR.** The dispatcher reports
  the first failure of a kind at ERROR and the ones that follow at WARNING, so
  the mirror forwards the news and not the outage repeated once per bar. The
  level returns to ERROR as soon as the failure changes or a publish succeeds.
  `failed` and `last_error` on `/status` are unchanged.
- **A first connect that fails is now retried.** nats-py's reconnect loop
  belongs to a client that a failed `connect()` never created, so an ingester
  started before its NATS server stayed cut off from it **for the whole run** —
  the comment in `runtime.py` claiming otherwise was wrong. `NatsConnection`
  now keeps dialling in the background every `NATS_RECONNECT_TIME_WAIT`, and a
  late connect is reported and hooked like a reconnect. The give-up watchdog is
  unchanged, so a NATS that never answers still stops the service.
- **The history responder subscribes on that late connect.** `start()` raises
  when NATS is down and there is then nothing for nats-py to re-establish; the
  first connect that works now subscribes and announces, instead of leaving the
  ingester publishing bars and answering no history request until a restart.

### Added — starting the service: foreground or detached

- **`make start`** (`uv run python -m ingester.start`) now starts the ingester
  as a **detached background process** and returns the shell: it outlives the
  terminal that launched it (`DETACHED_PROCESS` on Windows, a new session
  elsewhere) and appends whatever it writes to the console — an import error, a
  start-up traceback — to `<LOG_DIR>/ingester.out.log`, next to the application
  logs. It refuses to start when `APP_PORT` is already held, checked with the
  same probe `make stop` uses, so a second run cannot die on *address already
  in use* seconds after the command reported success. No PID file is written:
  the port already identifies the process, and `make stop` is what stops it.
  `make start PORT=8091` checks another port for a one-off.
- **`make dev`** is the blocking foreground run (`uv run python -m ingester`),
  with its logs on the console and Ctrl-C stopping it in order. `make run` is
  kept as an alias of it, so the older name keeps working.
- **Why.** `make start` used to be an alias of `make run`, i.e. blocking, which
  is wrong for the name: starting a service should hand the shell back. The two
  modes are now separate commands, and starting reports the PID it spawned —
  readiness is still `GET /health`.

### Added — stopping the service: by hand, and by itself

- **`make stop`** (`uv run python -m ingester.stop`) force-stops a running
  ingester: it reads `APP_PORT` from `.env` and kills **every** process whose
  socket has that TCP port as its **local** port — `taskkill /F /T` on
  Windows, `SIGKILL` elsewhere — so a detached or wedged run cannot keep the
  port and block the next `make run`. Owners are read with `netstat -ano`
  (Windows) or `ss 'sport = :<port>'`, falling back to
  `lsof -iTCP:<port> -sTCP:LISTEN`: a process merely *connected* to someone
  else's `:8090` is never killed, and PID 0/4 are skipped. Nothing else is
  filtered — whatever else holds that port goes down with it.
  `make stop PORT=8091` overrides the port for a one-off. The kill skips the
  ordered shutdown (no *stopped* message, no NATS drain); nothing is buffered
  to disk, so no bar is lost — Ctrl-C is still the way to stop it politely.
- **The ingester now stops itself when NATS stays unreachable.** New
  `NATS_GIVE_UP_AFTER_ATTEMPTS` (300) and `NATS_GIVE_UP_WINDOW_SECONDS`
  (1800): one failed attempt is counted per `NATS_RECONNECT_TIME_WAIT` while
  the link is down, and once that many land inside the rolling window the
  process posts **NATS Unreachable — Ingester Stopping** and shuts down
  through the normal lifecycle, so the message is delivered before it goes. It
  does **not** restart itself — an operator does, by hand. A reconnect does
  not clear the count, so a link that keeps flapping inside the window gives
  up too; `0` disables it entirely.
- **Why.** An ingester that cannot reach NATS publishes nothing. Retrying
  silently for hours leaves a process that looks alive, answers `/health` and
  delivers no bars; being plainly down, with a reason in the chat, is what an
  operator can act on. The watchdog polls the connection instead of counting
  nats-py's `error_cb`: that callback also fires on a healthy connection, and
  never fires at all when the *first* connect was abandoned — which is exactly
  the case where the process would otherwise run blind forever.
- **Under a supervisor**, configure it to leave the process down after this
  shutdown; an automatic restart only dials the same dead NATS again.

### Changed — history is asked for, not pushed at start-up (breaking)

- **A gateway publishes nothing at start-up.** The first read only records the
  latest closed bar; `backfill_on_start` is gone from `[mt5]` and `[binance]`,
  and with it the warm-up window. A subscriber that needs the bars from before
  it was listening sends a **history request** and gets them in one reply — it
  decides which series and how many bars, and the ingester holds no warm-up
  setting.
- **New: `<rpc_prefix>.history.<gateway>.<symbol>.<timeframe>`**, core NATS
  request/reply. Body `{symbol, timeframe, count}`; reply `{status, bars, …}`
  with the same `bar` objects a `bar.closed` event carries, closed bars only,
  oldest first. Contract in `ingester/schemas/history_schema.py`, samples in
  `examples/nats/history.*.json`. Refusals are answered too, with a `code` that
  says whether asking again can help.
- **New: `<rpc_prefix>.online.<gateway>`**, an announcement published once a
  gateway is answering requests and again after every NATS reconnect
  (`examples/nats/ingester.online.mt5.json`). A subscriber starts without the
  ingester, asks nothing on a timer, and reacts to this by checking its
  windows and requesting the short ones.
- **`warmup_bar`, `warmup_index` and `warmup_total` are gone from
  `bar.closed`.** Every message on that subject is a close. `SCHEMA_VERSION`
  stays `1.0.0`: a decoder that read a missing `warmup_bar` as `false` — which
  the contract required — reads the same bars it always did. The two entries
  that introduced those fields are dropped from this changelog rather than
  listed and then undone: nothing was released carrying them.
- **Why.** A window sent as numbered events has to be reassembled, and JetStream
  de-duplicates on `event_id`: a restart inside `NATS_DUPLICATE_WINDOW_SECONDS`
  had every warm-up bar dropped by the stream, and a restart after it lost the
  newest ones — including the bar that ends the window, which left the
  subscriber buffering a batch that never completed. It also triggered on the
  wrong side: the ingester only knows when *it* restarted, not when the
  subscriber's window is short.
- **Settings.** `[mt5] warmup_bars` is renamed **`recovery_bars`** — it only
  ever sized the read that recovers bars after a terminal disconnect.
  `[binance] warmup_bars` is removed. A market file still carrying
  `backfill_on_start` or `warmup_bars` is refused at start-up as an unknown
  key. New in `.env`: `NATS_RPC_SUBJECT_PREFIX` (blank =
  `<NATS_SUBJECT_PREFIX>_RPC`; refused when it sits under
  `NATS_SUBJECT_PREFIX`, where the stream would capture requests),
  `NATS_HISTORY_MAX_BARS` (5000), `NATS_HISTORY_TIMEOUT` (20s).
- **No new network exposure.** The subscription rides the connection the
  publisher already holds; nothing inbound is opened. Each gateway joins the
  queue group `<rpc_prefix>-history-<gateway>`.
- **MT5 history is read on the gateway thread**, between two polls
  (`ThreadedIngestion._call_on_thread`), and is served without waiting out the
  poll interval. `/status` gains `history_served` / `history_refused`.
- **The JetStream ack is read.** A publish the stream drops as a duplicate is
  logged as `JetStream DROPPED … — no consumer receives this message` instead
  of `Published`.

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
  | `MT5_CATCHUP_BARS` | `[mt5] recovery_bars` (renamed twice — see the top entry) |
  | `MT5_BACKFILL_ON_START` | removed — see the top entry |
  | `MT5_RECONNECT_INTERVAL_SECONDS` | `[mt5] reconnect_interval_seconds` |

  `MT5_TERMINAL_PATH`, `MT5_LOGIN`, `MT5_PASSWORD`, `MT5_SERVER` and
  `MT5_TIMEOUT_MS` **stay in `.env`**: they are secrets and host paths, and the
  market files are meant to be readable and diffable.
- **`catchup_bars` is now `recovery_bars`** in `[mt5]`, and is gone from
  `[binance]`. (It was renamed to `warmup_bars` first; the top entry renamed it
  again, once it was clear the key only ever sized an outage re-read.) The old
  key is refused as an unknown key, so a market file still carrying it is
  reported as **Ingester Degraded** — rename it in your own `config/*.toml`
  after pulling. The published payload is unaffected: it is
  how many closed bars a read covers, never a field on the wire.
- `config/*.toml` is git-ignored; `config/forex.example.toml` and
  `config/crypto.example.toml` are the committed templates. Copy them after
  pulling: a market with no file is reported as **Ingester Degraded**.

The published payload is unchanged — `SCHEMA_VERSION` stays `1.0.0` and
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

### Added (Binance)

- **`[binance].backfill_on_start` and `[binance].warmup_bars`** (both removed
  again before release — the top entry replaced them with history requests,
  answered from the same REST endpoint). The kline
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
- **`[mt5].backfill_on_start`** (removed again before release — the top entry
  replaced it with history requests). Publishes the closed bars found at
  start-up instead of only recording them, so a crash no longer silently skips
  the bars that closed while the process was down. Bounded by `warmup_bars`;
  pair it with JetStream, which drops the replayed duplicates by `event_id`.
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
