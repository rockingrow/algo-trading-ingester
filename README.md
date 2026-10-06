# Algo Trading Ingester

A market-data gateway for the algo-trading ecosystem. It watches upstream venues
for **closed bars**, normalises them into **one canonical schema**, and
publishes them **one way** to NATS for
[`quant-trading-engine`](https://github.com/rockingrow/quant-trading-engine)'s
`qte-ingest` to consume. Service status goes to Telegram.

One process runs several **markets** side by side — `SOURCE_MARKET=forex,crypto`
in `.env`, one `config/<market>.toml` per market:

| Market | File | Gateway | Notes |
| --- | --- | --- | --- |
| `forex` | `config/forex.toml` | `[mt5]` | MetaTrader 5, Windows, needs a running terminal |
| `crypto` | `config/crypto.toml` | `[binance]` | Binance kline websocket, any OS, no API key |

Each gateway owns its own source and its own publish queue, so a venue that
drops never touches the other market's bars.

---

## ⚡ Quick Start

### 1. Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)
- A NATS server (JetStream only if `NATS_JETSTREAM_ENABLED=true`)
- For the `forex` market (MT5 gateway): **Windows** with the MetaTrader 5
  terminal installed and logged in to your broker
- For the `crypto` market (Binance gateway): outbound access to
  `stream.binance.com` — nothing else, the kline streams are public

### 2. Install

```bash
git clone https://github.com/rockingrow/algo-trading-ingester
cd algo-trading-ingester

# Process settings and secrets: NATS, Telegram, SOURCE_MARKET, MT5 credentials
cp .env.example .env

# What each market ingests: symbols, timeframes, one [gateway] table per venue
cp config/forex.example.toml  config/forex.toml
cp config/crypto.example.toml config/crypto.toml
# or: make forex / make crypto — the same copy, but never over an existing file

uv sync                # or: make install-dev (adds ruff, pytest)
```

`config/*.toml` is git-ignored — it is yours. Only the `*.example.toml`
templates are committed, so pulling never overwrites your symbols.

The `MetaTrader5` package is only installed on Windows (it has no other wheels);
on Linux or macOS the `forex` market reports itself degraded and the `crypto`
market keeps ingesting.

### 3. Run

```bash
uv run python -m ingester   # or: make run
```

- `GET /health` — liveness + NATS connection state
- `GET /status` — per-gateway status (with its market), symbols, timeframes,
  publish counters and the newest bar per stream
- `/docs` — only when `APP_DOCS_ENABLED=true`

Response shapes are under [HTTP endpoints](#-http-endpoints).

### 4. Test

```bash
uv run pytest               # or: make test — any OS, both venues are faked
```

---

## 🏗️ Architecture

```mermaid
flowchart LR
    subgraph win["Windows host"]
        MT5[("MT5 terminal")]
    end
    BIN[("Binance<br/>kline websocket")]

    subgraph ing["algo-trading-ingester (this repo)"]
        direction LR
        subgraph fx["forex — config/forex.toml"]
            T["mt5-ingestion thread<br/>connect → poll → reconnect"]
            DTO["Mt5RateDTO<br/>→ canonical Bar"]
            T --> DTO
        end
        subgraph cr["crypto — config/crypto.toml"]
            W["binance-ingestion task<br/>connect → read → reconnect"]
            KDTO["BinanceKlineDTO<br/>→ canonical Bar"]
            W --> KDTO
        end
        Q[["asyncio.Queue<br/>(one per gateway)"]]
        D["dispatcher"]
        P["NatsPublisher<br/>(EventPublisher)"]
        N["QueuedNotifier → Telegram<br/>(Notifier)"]
        API["FastAPI<br/>/health /status"]
        DTO --> Q
        KDTO --> Q
        Q --> D --> P
        T -. status changes .-> N
        W -. status changes .-> N
    end

    MT5 -- "MetaTrader5 API" --> T
    BIN -- "wss combined stream" --> W
    P == "INGEST.bar.closed.&lt;gw&gt;.&lt;symbol&gt;.&lt;tf&gt;" ==> NATS{{NATS}}
    NATS ==> QTE["quant-trading-engine<br/>qte-ingest"]
    N --> TG["Telegram"]
```

### Design

| Concern | Where | Pattern |
| --- | --- | --- |
| Lifecycle, hand-off queue, publishing, status + notifications | `core/ingestion.py` → `BaseIngestion` | Template Method |
| Blocking SDK on a dedicated thread with reconnects | `core/ingestion.py` → `ThreadedIngestion` | Template Method |
| Asyncio feed on one task with reconnects | `core/ingestion.py` → `AsyncStreamIngestion` | Template Method |
| Per-stream de-duplication of emitted bars | `core/ingestion.py` → `BaseIngestion._is_new_bar` / `_remember_bar` | — |
| Pick gateways by name, once per market that enables them | `core/factory.py` → `IngestionFactory` | Factory / Registry |
| Market files → validated per-gateway settings | `settings.py` → `load_market`, `GATEWAY_SETTINGS` | Registry |
| Contracts between layers | `interfaces/` (`EventPublisher`, `Notifier`, `Ingestion`, `BarDTO`, `LogForwarder`) | Interface (Protocol) / DIP |
| Venue payload → canonical schema | `gateways/<market>/<venue>/dto.py`, extending `core/dto.py` → `BaseBarDTO` | DTO |
| Venue business logic only | `gateways/<market>/<venue>/ingestion.py` | — |
| Venue SDK / socket / REST seam, so tests need no network | `gateways/forex/mt5/terminal.py`, `gateways/crypto/binance/{stream,history}.py` | Adapter (Protocol) |
| Non-blocking Telegram | `services/notification_service.py` → `QueuedNotifier` | Decorator |
| Wiring concrete classes | `providers.py` | Composition root |

A gateway never touches NATS or Telegram, and the core never touches a venue. A
gateway's **market** is the file its table was read from, so `[mt5]` in
`config/forex.toml` is forex and the table can never contradict it.

### MT5 bar-close detection

Keys below are from the `[mt5]` table of a market file, e.g. `config/forex.toml`.

MetaTrader5's Python API has no callbacks, so the MT5 gateway polls from its
own thread (`mt5-ingestion`). Every `poll_interval_seconds` it reads the newest
**completed** bars (`copy_rates_from_pos(symbol, tf, 1, warmup_bars)` —
position 0 is the bar still forming) and emits every bar newer than the last
one it emitted.

- **Symbols** are named bare (`XAUUSD`). The gateway asks the terminal what
  this broker calls the instrument — Exness sells it as `XAUUSDm` — and reads
  bars from that name, while the published `symbol` stays the configured one,
  so `event_id` survives a change of broker. A fully spelled `XAUUSDm` is used
  verbatim; `symbol_suffix` settles a tie between, say, `EURUSDm` and
  `EURUSDc`.
- **Start-up** only records the latest closed bar, so a restart does not
  re-publish history. With `backfill_on_start = true` it publishes that whole
  window instead, recovering the bars a crash would otherwise skip — flagged
  `warmup_bar: true` and numbered `warmup_index` of `warmup_total`, oldest
  first, so a subscriber knows which bar ends the window. JetStream drops the
  replayed bars that fall inside its duplicate window; the older ones arrive
  again, see [Delivery](#-nats-contract).
- **Gap recovery**: bars that closed during a terminal disconnect are
  published, oldest first, after reconnecting (up to `warmup_bars`). They are
  late, not warm-up — only the start-up window carries `warmup_bar: true`.
- **Server time**: MT5 stamps bars in trade-server time. Set `server_timezone`
  so they are converted to real UTC.
- A close is seen when the next bar opens (its first tick), plus up to one poll
  interval.

### Binance bar-close detection

Keys below are from the `[binance]` table of a market file, e.g.
`config/crypto.toml`.

Binance pushes a kline update several times a second and flags the final one
with `k.x == true`, so nothing is polled: the gateway opens **one** combined
websocket (`/stream?streams=btcusdt@kline_1m/…`) carrying every symbol ×
timeframe and emits the bars Binance marks closed.

- **Symbols** are written the way Binance spells them (`BTCUSDT`). The stream
  name is lower-cased for the subscription; the published `symbol` is the
  configured one.
- **Timeframes** are the canonical labels (`M15`); the gateway maps them onto
  Binance's own spelling (`15m`) and back.
- **`close_time`** is derived as `open_time + timeframe`, not taken from
  Binance's `T` — which is one millisecond short of the next bar's open. A
  subscriber comparing venues should not have to know that.
- **Start-up**: the socket only carries bars that close while it is connected,
  so by default the bars that closed before this process started are not
  published. With `backfill_on_start = true` the gateway reads the last
  `warmup_bars` closed bars per stream from the REST endpoint at
  `klines_url` — after the socket is open, so no bar falls between the two —
  and publishes them first, giving a subscriber backfill and live bars as one
  continuous series. Only streams nothing has been published for yet are read,
  so a reconnect re-reads nothing; a REST failure is logged and skipped rather
  than costing the live socket. Those bars carry `warmup_bar: true` and their
  `warmup_index` of `warmup_total`, oldest first; what the socket delivers
  carries neither. JetStream drops the replayed bars that fall inside
  its duplicate window; the older ones arrive again, see
  [Delivery](#-nats-contract).
- **De-duplication**: the newest open time emitted per stream is remembered, so
  a reconnect that replays a bar, or a repeated final update, publishes once.
- **Reconnects**: Binance closes a connection after 24 hours, and a socket that
  goes quiet for `idle_timeout_seconds` is dropped; either way the gateway
  dials again every `reconnect_interval_seconds`. Keep `idle_timeout_seconds`
  well above the slowest expected update, or a healthy stream is cut.
- **No credentials**: the kline streams and the klines endpoint are both
  public. Point `ws_url` at `wss://testnet.binance.vision/stream` and
  `klines_url` at `https://testnet.binance.vision/api/v3/klines` to test
  against the testnet — keep the two on the same product, or they disagree
  about which book the bars came from.

---

## 📨 NATS contract

**Subject** — `<NATS_SUBJECT_PREFIX>.bar.closed.<gateway>.<symbol>.<timeframe>`

```text
INGEST.bar.closed.mt5.XAUUSD.M15
INGEST.bar.closed.binance.BTCUSDT.M15
INGEST.bar.closed.>              # everything
INGEST.bar.closed.mt5.*.H1       # every MT5 symbol, H1 only
INGEST.bar.closed.binance.>      # the whole crypto market
```

Characters NATS reserves are replaced with `_` in the subject token only
(`XAUUSD.m` → `XAUUSD_m`); the payload keeps the symbol verbatim.

**Payload** — `BarClosedEvent` (`ingester/schemas/market_event_schema.py`), JSON,
all times UTC. Full examples:
[`bar.closed.mt5.json`](examples/nats/bar.closed.mt5.json),
[`bar.closed.binance.json`](examples/nats/bar.closed.binance.json),
[`bar.closed.mt5.warmup.json`](examples/nats/bar.closed.mt5.warmup.json).

| Field | Meaning |
| --- | --- |
| `schema_version` | `1.0.0` — bumped on breaking changes |
| `event_id` | `<gateway>:<symbol>:<tf>:<open epoch>` — deterministic; de-dup key and JetStream `Nats-Msg-Id` |
| `event_type` | `bar.closed` |
| `source` | `gateway`, `market` (the file it was configured in), `ingester_id`, `venue` (MT5 trade server, or the Binance endpoint host) |
| `emitted_at` | when this process published the bar, UTC — wall-clock, so it is deliberately **not** part of `event_id` |
| `warmup_bar` | `true` only on the bars this process read back at start-up (the warm-up window); `false` on everything the live feed delivers |
| `warmup_index` | position of this bar in that window, `1` … `warmup_total`, oldest first; `null` on a live bar |
| `warmup_total` | bars in that window — `warmup_index == warmup_total` is the last one; `null` on a live bar |
| `symbol`, `timeframe` | as configured; timeframe ∈ `M1 M5 M15 M30 H1 H4 D1 W1` |
| `bar` | `open_time`, `close_time`, `open`, `high`, `low`, `close`, `volume`, `tick_count`, `quote_volume`, `spread` |

Venue-specific fields are `null` rather than zero when a venue does not have
them (MT5 has no `quote_volume`, Binance has no `spread`).

`warmup_bar` describes **this delivery**, not the bar: the same bar can reach a
subscriber live from one process and again as warm-up from the next, and with
JetStream the copy that lands first is the one kept — so de-duplicate on
`event_id` as before and read `warmup_bar` only to decide whether to act on a
bar. A payload without the field is a `false`. None of the three warm-up fields
is part of `event_id`, for the same reason and one more: the numbers describe
the window, so the same bar is `150` of `150` for one start-up and `1` of `150`
for the next.

**Warm-up order** — a gateway publishes the whole warm-up window **before** any
live bar, oldest first, numbered `1` … `warmup_total` with no gaps. So a
subscriber ends its warm-up on exactly one message, the one where
`warmup_index == warmup_total`, and everything after it on that subject is live
(`warmup_bar: false`). The order is guaranteed: one publish queue per gateway,
drained one event at a time.

What the ingester guarantees, and what it does not:

- **The series is per `(symbol, timeframe)`**, which is per subject. With
  `symbols = ["XAUUSD", "USOIL"]` each gets its own `1` … `total`, so warm-up
  ends per subject rather than once for the gateway. The two series do not mix
  on one subject, but they do interleave in time.
- **`warmup_total` is the window the venue actually returned**, bounded by the
  market file's `warmup_bars` and shorter when there is less history than that
  — a recently listed symbol, a broker with a thin archive. It is deliberately
  not the configured number: a subscriber waiting for `150` of `150` from a
  broker that only has `143` bars would wait forever.
- **A window is all-or-nothing.** Every bar is converted before the first is
  published, so a record the schema rejects half-way through publishes nothing
  instead of a series that stops short of the `warmup_total` it announced. The
  stream keeps its place, so the next poll (MT5) or reconnect (Binance) retries
  the whole window.
- **A window is not guaranteed to arrive at all**, so do not block on one
  forever. There is none with `backfill_on_start = false`, none for a stream
  whose history cannot be read, and none for a Binance stream whose REST
  backfill fails — in each case live bars still flow, carrying
  `warmup_bar: false` and no numbers. Read that as *no warm-up data*, not
  *warm-up still pending*.
- **Delivery is only as reliable as the transport.** The numbering is
  contiguous as published — one queue per gateway, drained one event at a time
  — but with core NATS a publish that fails is logged, counted in `/status` as
  `failed`, and not retried, which can take a bar out of the series. Use
  JetStream (`NATS_JETSTREAM_ENABLED=true`) if the subscriber gates on
  `warmup_index == warmup_total`.

**Delivery** — core NATS by default (fire-and-forget). With
`NATS_JETSTREAM_ENABLED=true`, bars are persisted on a stream named after
`NATS_SUBJECT_PREFIX` (created if missing, never reconfigured) and
de-duplicated by `event_id`, which rides as `Nats-Msg-Id`. The stream name is
derived from the prefix rather than configured separately, so it can never
drift from the subjects it has to carry. One stream and one subject tree carry
every market: `market` and `gateway` tell a subscriber what it is looking at,
so nothing downstream branches per venue.

De-duplication only reaches back `NATS_DUPLICATE_WINDOW_SECONDS` (default
`120`). A warm-up window is usually older than that — 150 bars of M15 is 37
hours — so a restart does **not** silently collapse into the bars already
published: those messages arrive again, carrying `warmup_bar: true` and their
position in the new window. Either
raise the window to cover the span `backfill_on_start` can replay (it is fixed
when the stream is created and never reconfigured), or let the subscriber use
`warmup_bar` and its own `event_id` bookkeeping. Retention is
`NATS_STREAM_MAX_AGE_SECONDS`, 7 days by default.

---

## 🔔 Telegram notifications

With `TELEGRAM_ENABLED=true`, the bot posts to every chat in `TELEGRAM_CHAT_IDS`:

- ingester **running** (endpoint, NATS subject, symbols × timeframes) /
  **stopped** / **degraded** (started, but NATS is down, a market file would
  not load, a gateway refused to start, or nothing is ingesting at all)
- each gateway's status changes: `starting`, `running`, `disconnected` (with the
  reason), `failed` (it would not start), `stopped` — the **running** message
  names each stream as `<market>/<GATEWAY>`, so it is clear which file it came
  from
- NATS **disconnected** / **reconnected**

Messages go through a bounded queue, so a slow Telegram never delays a bar.

### Errors in their own chat

`TELEGRAM_LOG_ERRORS_ENABLED=true` mirrors every `ERROR` log record into
`TELEGRAM_LOG_CHAT_IDS` (falling back to `TELEGRAM_CHAT_IDS`), with its own
optional bot token and its own queue, so a burst of failures cannot delay a
lifecycle message. Identical records inside `TELEGRAM_LOG_DEDUP_WINDOW` seconds
are dropped — a failing poll repeats every interval. Only `ingester.*` records
are forwarded; uvicorn keeps its own loggers.

### Staying up

The ingester does not exit because a dependency is down. If NATS is unreachable
at start-up, a market's TOML file is missing or malformed, or a gateway refuses
to start, it logs the error, posts an **Ingester Degraded** message and keeps
running: NATS reconnects underneath, the markets that *did* load keep ingesting,
and a supervisor restart would only drop more bars. One malformed record on one
symbol — or one unreadable websocket frame — is logged and skipped without
starving the rest.

---

## 🩺 HTTP endpoints

Read-only — the real output goes to NATS. `/docs` and `/openapi.json` are served
only when `APP_DOCS_ENABLED=true`.

`GET /health` — liveness for a supervisor or uptime check:

```json
{ "status": "ok", "version": "0.1.0", "nats_connected": true }
```

`GET /status` — what every gateway is doing, one entry per gateway that started:

```json
{
  "instance_id": "vps-mt5-01",
  "version": "0.1.0",
  "schema_version": "1.0.0",
  "nats": { "connected": true, "subject_filter": "INGEST.>" },
  "gateways": [
    {
      "gateway": "mt5",
      "status": "running",
      "status_detail": null,
      "market": "forex",
      "venue": "Broker-Server-Demo",
      "symbols": ["XAUUSD", "USOIL"],
      "timeframes": ["M15"],
      "published": 312,
      "failed": 0,
      "last_published_at": "2026-09-28T10:15:00.412000+00:00",
      "last_error": null,
      "last_bar_open_time": { "XAUUSD:M15": "2026-09-28T10:00:00+00:00" }
    }
  ]
}
```

`status` is one of `idle`, `starting`, `running`, `disconnected`, `failed`,
`stopped`; `status_detail` carries the reason on `disconnected` and `failed`,
and is `null` otherwise. `published` and `failed` count NATS publishes since
start-up, `last_error` is the last publish failure, and `last_bar_open_time` is
the newest bar emitted per `<symbol>:<timeframe>` — the quickest way to see
whether a stream has gone quiet. A market whose file would not load has no entry here at
all; the log and the **Ingester Degraded** message say why.

---

## ⚙️ Configuration

Configuration lives in two places, because the two change for different
reasons.

### 1. `.env` — this process and this host

The FastAPI server, logging, NATS, Telegram, which markets to run, and venue
**credentials**. See [`.env.example`](.env.example) for every key with comments.

| Key | Default | What it decides |
| --- | --- | --- |
| `SOURCE_MARKET` | `forex` | Which market files to load and run, comma-separated (`forex,crypto`) |
| `SOURCE_CONFIG_DIR` | `config` | Where those files live |
| `APP_NAME` | `algo-trading-ingester` | Name in logs and Telegram headers |
| `APP_HOST`, `APP_PORT` | `0.0.0.0`, `8090` | Where the HTTP surface listens |
| `APP_INSTANCE_ID` | hostname | `source.ingester_id` on every payload — set it when several VPS publish |
| `APP_DOCS_ENABLED` | `false` | Serves `/docs` and `/openapi.json` |
| `SCHEMA_VERSION` | `1.0.0` | Stamped as `schema_version`; keep it in step with what `qte-ingest` accepts |
| `LOG_LEVEL`, `LOG_DIR` | `INFO`, `logs` | Console + daily file logging |
| `NATS_HOST`, `NATS_PORT`, `NATS_TOKEN` | `localhost`, `4222`, blank | Connection; blank token = no auth |
| `NATS_SUBJECT_PREFIX` | `INGEST` | First subject token, and the JetStream stream name |
| `NATS_CONNECT_TIMEOUT`, `NATS_RECONNECT_TIME_WAIT` | `5.0`, `2.0` | Seconds to dial, seconds between reconnects |
| `NATS_JETSTREAM_ENABLED` | `false` | `false` = core NATS, fire-and-forget. `true` = persisted and de-duplicated |
| `NATS_PUBLISH_TIMEOUT` | `5.0` | Seconds one JetStream publish ack may take |
| `NATS_STREAM_MAX_AGE_SECONDS` | `604800` | Stream retention, 7 days. Set at creation only |
| `NATS_DUPLICATE_WINDOW_SECONDS` | `120` | How far back JetStream de-duplicates on `event_id` — see [Delivery](#-nats-contract) |
| `TELEGRAM_ENABLED`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_IDS` | `false`, blank, blank | Status chat; ids comma-separated, `-100…_42` posts into a forum topic |
| `TELEGRAM_HTTP_TIMEOUT`, `TELEGRAM_QUEUE_SIZE` | `5.0`, `100` | Per-send timeout and the queue that keeps Telegram off the bar path |
| `TELEGRAM_LOG_ERRORS_ENABLED`, `TELEGRAM_LOG_CHAT_IDS`, `TELEGRAM_LOG_BOT_TOKEN` | `false`, falls back to `TELEGRAM_CHAT_IDS`, falls back to `TELEGRAM_BOT_TOKEN` | Mirror of every `ingester.*` `ERROR` record into its own chat |
| `TELEGRAM_LOG_DEDUP_WINDOW` | `60` | Seconds an identical error record is suppressed |
| `MT5_LOGIN`, `MT5_PASSWORD`, `MT5_SERVER` | blank | Blank = attach to whatever account the terminal is logged in to |
| `MT5_TERMINAL_PATH`, `MT5_TIMEOUT_MS` | blank, `60000` | `terminal64.exe` path (blank = let the package find it), and the init timeout |

### 2. `config/<market>.toml` — what that market ingests

One file per market named in `SOURCE_MARKET`, one `[gateway]` table per venue
inside it, `enable` first. The **market is the file's name** and is never
written in the table. Templates:
[`forex.example.toml`](config/forex.example.toml),
[`crypto.example.toml`](config/crypto.example.toml). Your own `config/*.toml`
is git-ignored.

```toml
# config/forex.toml
[mt5]
enable = true
symbols = ["XAUUSD", "EURUSD"]   # bare names; the broker affix is detected
symbol_suffix = ""               # "m" to force Exness naming
timeframes = ["M1", "M15", "H1"]
server_timezone = "Europe/Athens"
poll_interval_seconds = 1.0
warmup_bars = 5
backfill_on_start = false
reconnect_interval_seconds = 5.0
```

```toml
# config/crypto.toml
[binance]
enable = true
symbols = ["BTCUSDT", "ETHUSDT"]
timeframes = ["M1", "M15", "H1"]
ws_url = "wss://stream.binance.com:9443/stream"
backfill_on_start = true
warmup_bars = 16                # only used by the backfill; max 1000
klines_url = "https://api.binance.com/api/v3/klines"
http_timeout_seconds = 10.0
reconnect_interval_seconds = 5.0
ping_interval_seconds = 20.0
ping_timeout_seconds = 20.0
idle_timeout_seconds = 90.0
```

`enable = false` (or a missing `enable`) leaves that gateway out; dropping a
market from `SOURCE_MARKET` stops reading its file at all.

What a market file is **not** allowed to say:

- `market` — it is the file's name, so a table could only contradict it.
- `login`, `password`, `server`, `terminal_path`, `timeout_ms` — credentials
  and host paths belong in `.env` as `MT5_…`. These files are meant to be read
  and diffed.
- a key no gateway has (a typo), or an unknown `[table]`.

Each is refused at start-up instead of being quietly accepted, as is one
gateway enabled in two markets — MetaTrader 5 keeps a single process-wide
terminal session, and `event_id` carries no market, so the second one is
refused and reported.

A file that is missing, unparseable or refused for any of the above, and a
process where nothing at all ends up ingesting, is reported as **Ingester
Degraded** — the markets that loaded still run.

---

## 🧩 Adding a gateway

A new venue needs only its own business logic. Take Kraken as the example:

1. **Enum** — add `KRAKEN = "kraken"` to `GatewayEnum` in `schemas/enums.py`
   (it becomes a subject token, so the value is contract).
2. **Settings** — in `settings.py`, subclass `GatewaySettings` with
   `env_prefix="KRAKEN_"` (it inherits `ENABLE`, `SYMBOLS`, `TIMEFRAMES`,
   `MARKET`), then add one row to `GATEWAY_SETTINGS`. The prefix only serves
   keys that belong in `.env` — credentials and host paths; everything else
   comes from the market file.
3. **Venue seam** — `gateways/crypto/kraken/{terminal,stream}.py` (the folder of
   the market the venue natively serves; a new market is a new folder with its
   own `__init__.py`): a `Protocol` for
   the slice of the SDK or socket you use, so tests can fake it (see
   `Mt5Terminal`, `KlineStream`).
4. **DTO** — `gateways/crypto/kraken/dto.py`: a `BaseBarDTO` subclass for the
   raw payload that implements `to_bar(timeframe) -> Bar`.
5. **Ingestion** — `gateways/crypto/kraken/ingestion.py`:
   - an asyncio websocket: subclass `AsyncStreamIngestion` and implement
     `connect`, `receive`, `handle`, `disconnect`, like Binance; `handle`
     calls `self.emit_bar(symbol, timeframe, dto.to_bar(timeframe))` for each
     closed bar — from a start-up backfill path pass `warmup_bar=True` with
     `warmup_index=` / `warmup_total=`, numbering the window `1` … `total`
     oldest first, so a subscriber can tell warm-up from a live close and see
     where the window ends. The schema rejects a half-numbered series;
   - a blocking SDK: subclass `ThreadedIngestion` and implement `connect`,
     `poll`, `disconnect`, like MT5.
   Guard each bar with `self._is_new_bar(...)` / `self._remember_bar(...)` so
   a replayed bar is emitted once. Raise `GatewayConnectionError` when the
   venue drops; use
   `self._set_status(...)` for status changes — the core notifies Telegram.
6. **Register** — one line in `providers.make_ingestion_factory()`:
   `factory.register(GatewayEnum.KRAKEN, build_kraken_ingestion)`. The builder
   reads its settings with `context.config_as(KrakenSettings)` and passes the
   shared publisher, notifier and instance id on with
   `**context.ingestion_dependencies()`.
7. **Enable it** — a `[kraken]` table with `enable = true` in the market file
   it belongs to, and that market in `SOURCE_MARKET`. The same builder serves
   every market that enables it.
8. **Template** — add the table to `config/<market>.example.toml`, commented.

Publishing, subjects, de-duplication, queueing, notifications and the HTTP
status endpoint come for free.

---

## 📁 Project structure

```text
algo-trading-ingester/
├── ingester/
│   ├── api/             # FastAPI routes: /health, /status
│   ├── core/            # Base ingestions, BaseBarDTO, IngestionFactory, errors
│   ├── gateways/        # One folder per market, one per venue inside it
│   │   ├── forex/
│   │   │   └── mt5/     # terminal adapter, broker-affix resolution, DTO,
│   │   │                # bar-close business logic
│   │   └── crypto/
│   │       └── binance/ # websocket + REST klines adapters, kline DTO,
│   │                    # bar-close business logic
│   ├── helpers/         # Telegram message templates, emoji
│   ├── interfaces/      # Protocols: EventPublisher, Notifier, Ingestion,
│   │                    # BarDTO, LogForwarder
│   ├── schemas/         # Canonical wire contract: enums, Bar, BarClosedEvent
│   ├── services/        # NATS connection/publisher, Telegram notifier
│   ├── app.py           # FastAPI application factory (lifespan)
│   ├── runtime.py       # Ordered start/stop of notifier, NATS, gateways
│   ├── providers.py     # Composition root + gateway registration
│   ├── settings.py      # Settings from .env + market files (config/*.toml)
│   ├── logger.py        # Console + daily file logging
│   ├── main.py          # Entrypoint (uvicorn)
│   └── __main__.py      # So `python -m ingester` works
├── config/              # Per-market gateway config; *.example.toml committed,
│                        # the operator's own *.toml git-ignored
├── examples/
│   ├── nats/            # Example payloads — the contract for qte-ingest
│   └── mt5/             # Raw MT5 rate samples, as the terminal returns them
├── tests/               # Pytest suite (both venues faked, runs on any OS)
├── logs/                # Daily log files (git-ignored)
├── .claude/             # Claude Code settings + SessionStart hook (uv sync)
├── AGENTS.md            # Shared instructions for coding agents
├── CLAUDE.md            # Claude Code specifics (imports AGENTS.md)
├── .env.example
├── changelog.md
├── Makefile
└── pyproject.toml
```
