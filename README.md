# Algo Trading Ingester

A market-data gateway for the algo-trading ecosystem. It watches upstream venues
for **closed bars**, normalises them into **one canonical schema**, and
publishes each one to NATS for
[`quant-trading-engine`](https://github.com/rockingrow/quant-trading-engine)'s
`qte-ingest` to consume. Bars go **one way**; the only thing it listens for is a
[history request](#history-requests) — the bars from before a subscriber was
listening are not pushed, they are asked for and answered in one reply. Service
status goes to Telegram.

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

Ctrl-C stops it in order (gateways → NATS drain → Telegram). When a run was
detached, or is wedged and still holding the port:

```bash
make stop                   # or: uv run python -m ingester.stop
make stop PORT=8091         # another port than APP_PORT, for a one-off
```

It reads `APP_PORT` from `.env` and force-kills **every** process whose socket
has that **local** port — `taskkill /F /T` on Windows, `SIGKILL` elsewhere — so
the next `make run` can bind. A client merely *connected* to some other host's
`:8090` is never touched. Nothing else is filtered, so give the ingester a port
of its own. A hard kill skips the ordered shutdown: no *stopped* message on
Telegram and no NATS drain. Published bars are already downstream and nothing is
buffered to disk, so no data is lost; use Ctrl-C when you want the notification.

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
**completed** bars (`copy_rates_from_pos(symbol, tf, 1, recovery_bars)` —
position 0 is the bar still forming) and emits every bar newer than the last
one it emitted.

- **Symbols** are named bare (`XAUUSD`). The gateway asks the terminal what
  this broker calls the instrument — Exness sells it as `XAUUSDm` — and reads
  bars from that name, while the published `symbol` stays the configured one,
  so `event_id` survives a change of broker. A fully spelled `XAUUSDm` is used
  verbatim; `symbol_suffix` settles a tie between, say, `EURUSDm` and
  `EURUSDc`.
- **Start-up** only records the latest closed bar: nothing is published from
  the first read, so a restart never re-publishes history. The bars a
  subscriber needs from before it was listening are not pushed — the gateway
  does not know who is listening or how long their indicator window is. They
  are [asked for](#history-requests), and answered in one reply.
- **Gap recovery**: bars that closed during a terminal disconnect are
  published, oldest first, after reconnecting (up to `recovery_bars`). Bars
  that closed while the *process* was down are not: the next close is
  published with a gap before it, and the subscriber fills that gap with a
  history request.
- **History** is read on this same thread, between two polls — the MetaTrader5
  package keeps thread-affine state. Any timeframe MT5 has may be asked for,
  not only the ones this gateway polls; the symbol must be one it is
  configured for.
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
  so the bars that closed before this process started are not published. A
  subscriber that needs them [asks](#history-requests) and is answered from the
  REST endpoint at `klines_url` — at most 999 closed bars per request,
  Binance's limit less the bar still forming, in any timeframe Binance has.
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
[`bar.closed.binance.json`](examples/nats/bar.closed.binance.json).

| Field | Meaning |
| --- | --- |
| `schema_version` | `1.0.0` — bumped on breaking changes |
| `event_id` | `<gateway>:<symbol>:<tf>:<open epoch>` — deterministic; de-dup key and JetStream `Nats-Msg-Id` |
| `event_type` | `bar.closed` |
| `source` | `gateway`, `market` (the file it was configured in), `ingester_id`, `venue` (MT5 trade server, or the Binance endpoint host) |
| `emitted_at` | when this process published the bar, UTC — wall-clock, so it is deliberately **not** part of `event_id` |
| `symbol`, `timeframe` | as configured; timeframe ∈ `M1 M5 M15 M30 H1 H4 D1 W1` |
| `bar` | `open_time`, `close_time`, `open`, `high`, `low`, `close`, `volume`, `tick_count`, `quote_volume`, `spread` |

Venue-specific fields are `null` rather than zero when a venue does not have
them (MT5 has no `quote_volume`, Binance has no `spread`).

Every message on this subject is a close: a bar the venue has just finished,
or one a reconnect recovered late. History is never replayed onto it, so there
is nothing on the wire to tell the two apart and no warm-up fields to read.

### History requests

A subscriber that needs the bars from before it was listening asks for them.
**The subscriber decides which series and how many bars; the ingester holds no
warm-up setting.**

**Subject** — `<rpc_prefix>.history.<gateway>.<symbol>.<timeframe>`, core NATS
request/reply:

```text
INGEST_RPC.history.mt5.XAUUSD.M15
```

`rpc_prefix` is `NATS_RPC_SUBJECT_PREFIX`, or `<NATS_SUBJECT_PREFIX>_RPC` when
that is blank. **It may not sit under `NATS_SUBJECT_PREFIX`** — the JetStream
stream listens on `<NATS_SUBJECT_PREFIX>.>` and would store every request as
market data and answer it with its own acknowledgement before the ingester
could. A prefix that does is refused at start-up.

**Request** — `HistoryRequest` (`ingester/schemas/history_schema.py`),
[`history.request.mt5.json`](examples/nats/history.request.mt5.json):

| Field | Meaning |
| --- | --- |
| `schema_version` | the caller's; not checked today |
| `request_id` | free text, echoed on the reply and in both logs |
| `symbol` | the bare name from the market file (`XAUUSD`), matched without regard to case |
| `timeframe` | any of `M1 M5 M15 M30 H1 H4 D1 W1` the venue has — not only the ones this gateway publishes |
| `count` | how many of the newest **closed** bars to return, `>= 1` |

**Reply** — one `HistoryReply`,
[`history.reply.mt5.json`](examples/nats/history.reply.mt5.json) /
[`history.reply.error.json`](examples/nats/history.reply.error.json):

| Field | Meaning |
| --- | --- |
| `status` | `ok` or `error` |
| `request_id`, `symbol`, `timeframe`, `requested` | the request, echoed — `symbol` as the market file spells it |
| `source` | same object as on a `bar.closed` event |
| `bars` | closed bars, oldest first, each the same `bar` object a `bar.closed` event carries — never the bar still forming |
| `truncated` | `true` when bars were held back: `count` was over `NATS_HISTORY_MAX_BARS`, or the reply would not fit in one NATS message. Fewer bars because the venue has no more history is **not** truncation |
| `error` | `{code, message}` on `status: "error"`, otherwise `null` |

| `error.code` | Meaning | Ask again? |
| --- | --- | --- |
| `unknown_symbol` | the symbol is not configured for this gateway | no |
| `bad_request` | the body is not a request this version reads | no |
| `unavailable` | the venue is disconnected, or returned nothing | yes |
| `timeout` | the venue did not answer within `NATS_HISTORY_TIMEOUT` | yes |
| `internal` | a bug here; the log has the traceback | yes |

**Announcement** — `<rpc_prefix>.online.<gateway>`, core NATS, an
`OnlineAnnouncement`
([`ingester.online.mt5.json`](examples/nats/ingester.online.mt5.json)):

```text
INGEST_RPC.online.mt5
```

Published once the gateway's request subscription is in place, and again after
every NATS reconnect (`reason`: `started` / `reconnected`), carrying `source`
and the `symbols` / `timeframes` it is configured for. It is how a subscriber
runs independently of the ingester: it starts on whatever it holds, asks
nothing on a timer, and when this arrives checks its windows and requests the
short ones. The announcement is flushed only after the subscription is, so a
request it provokes always finds a responder. It is not persisted — a
subscriber that was not listening learns the same thing by asking, since NATS
answers *no responders* at once when no ingester is subscribed.

What the ingester guarantees, and what it does not:

- **One reply, whole or absent.** There is no series to reassemble and no
  message of it that can be lost on its own. A caller that hears nothing
  retries the request.
- **Every request with a reply subject is answered**, the refusals included,
  so "the ingester said no" never looks like "the ingester is not running".
  With no ingester subscribed, NATS itself answers *no responders* at once.
- **Nothing inbound is opened.** The subscription rides the connection this
  process already dialled to publish; a request travels back down it.
- **One ingester answers.** Each gateway subscribes in the queue group
  `<rpc_prefix>-history-<gateway>`, so a fail-over pair answers a request once
  between them.
- **A reply always fits one message.** When it would exceed the server's
  `max_payload` the oldest bars are dropped and `truncated` is set — a window
  is read from its newest end.
- **Requests are served one at a time per gateway**, in arrival order, and for
  MT5 between two polls. A request never delays a live bar by more than one
  read of the terminal.
- **Not persisted.** Requests are core NATS even when bars go to JetStream; a
  request sent while the ingester is down is simply not answered.

**Delivery** — core NATS by default (fire-and-forget). With
`NATS_JETSTREAM_ENABLED=true`, bars are persisted on a stream named after
`NATS_SUBJECT_PREFIX` (created if missing, never reconfigured) and
de-duplicated by `event_id`, which rides as `Nats-Msg-Id`. The stream name is
derived from the prefix rather than configured separately, so it can never
drift from the subjects it has to carry. One stream and one subject tree carry
every market: `market` and `gateway` tell a subscriber what it is looking at,
so nothing downstream branches per venue.

De-duplication only reaches back `NATS_DUPLICATE_WINDOW_SECONDS` (default
`120`). The one time a gateway re-reads bars it may already have published is
after a venue outage, and how far back that goes is `recovery_bars` × the
timeframe — size the window to cover it (it is fixed when the stream is created
and never reconfigured), or a recovered bar is stored twice and the subscriber
de-duplicates on `event_id`. Retention is `NATS_STREAM_MAX_AGE_SECONDS`, 7 days
by default.

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
- NATS **disconnected** / **reconnected**, and **NATS Unreachable — Ingester
  Stopping** when it has given up and is shutting itself down (see
  [Staying up](#staying-up))

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

**One exception: NATS staying unreachable.** An ingester that cannot reach NATS
publishes nothing, so retrying quietly for hours only hides the outage. Once
`NATS_GIVE_UP_AFTER_ATTEMPTS` connection attempts have failed inside
`NATS_GIVE_UP_WINDOW_SECONDS` — by default 300 attempts, one per
`NATS_RECONNECT_TIME_WAIT`, so about ten minutes of being unreachable within any
half hour — it posts **NATS Unreachable — Ingester Stopping** and shuts itself
down the ordered way, so that message is actually delivered. It does **not**
restart itself: an operator starts it again once NATS is back. A reconnect does
not clear the count, so a link that keeps flapping inside the window gives up
too; `NATS_GIVE_UP_AFTER_ATTEMPTS=0` turns the whole thing off. Under a
supervisor that restarts the process, set the supervisor to leave it down —
otherwise it will be restarted into the same dead NATS.

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
| `NATS_GIVE_UP_AFTER_ATTEMPTS`, `NATS_GIVE_UP_WINDOW_SECONDS` | `300`, `1800` | Failed attempts in that window after which the service stops itself and waits for an operator — `0` = never, see [Staying up](#staying-up) |
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
recovery_bars = 5                # bars a terminal reconnect can recover
reconnect_interval_seconds = 5.0
```

```toml
# config/crypto.toml
[binance]
enable = true
symbols = ["BTCUSDT", "ETHUSDT"]
timeframes = ["M1", "M15", "H1"]
ws_url = "wss://stream.binance.com:9443/stream"
klines_url = "https://api.binance.com/api/v3/klines"   # history requests are answered from here
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
     closed bar. Publish nothing at start-up: history is asked for, so
     implement `_read_history(symbol, timeframe, count)` to return the newest
     closed bars from the venue's REST endpoint instead;
   - a blocking SDK: subclass `ThreadedIngestion` and implement `connect`,
     `poll`, `disconnect`, like MT5. Its `_read_history` hands the read to the
     gateway thread with `self._call_on_thread(...)` — never call a
     thread-affine SDK from the event loop.
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
