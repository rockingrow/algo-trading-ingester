"""
ingester/settings.py — Process settings from ``.env``, market settings from TOML.

Two layers, because they change for different reasons:

* **``.env``** — everything about *this process and this host*: the FastAPI
  server, logging, NATS, Telegram, the MT5 terminal credentials, and
  ``SOURCE_MARKET`` naming which markets to run. Grouped into focused
  ``*Settings`` sub-models (``settings.app.PORT``, ``settings.nats.url``), each
  carrying an ``env_prefix`` so the env var names stay flat and each one can be
  instantiated on its own in tests.
* **``config/<market>.toml``** — everything about *what to ingest*, per market.
  One file per market named in ``SOURCE_MARKET`` (``config/forex.toml``,
  ``config/crypto.toml``), one ``[gateway]`` table per gateway inside it, and
  ``enable`` in each table deciding whether that gateway runs. The market a
  gateway belongs to comes from the file it is written in, so it is never
  repeated — and never contradicted — inside the table.

The two meet in :class:`GatewaySettings`: the TOML table is passed as init
arguments, which outrank the environment in pydantic-settings, so operational
knobs come from the file while secrets (``MT5_LOGIN`` …) stay in ``.env``.

Nothing about what to ingest is hard-coded: symbols, timeframes, the enabled
markets and gateways, host/port and every NATS/Telegram knob are configuration.
"""

from __future__ import annotations

import re
import socket
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any, ClassVar

from pydantic import (
  Field,
  SkipValidation,
  ValidationError,
  field_validator,
  model_validator,
)
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from ingester.schemas.enums import GatewayEnum, MarketEnum, Timeframe

#: A NATS stream name cannot hold a dot, space, wildcard or slash; a derived
#: name keeps only what is always safe and replaces the rest.
_STREAM_NAME_UNSAFE = re.compile(r"[^A-Za-z0-9_-]")


def _config(prefix: str) -> SettingsConfigDict:
  """Shared sub-model config: read the same .env, ignore unrelated vars, and
  scope this group's fields to *prefix* so env var names stay flat."""
  return SettingsConfigDict(
    env_file=".env",
    env_file_encoding="utf-8",
    extra="ignore",
    env_prefix=prefix,
  )


def split_csv(value: Any) -> Any:
  """Turn ``"A, B,,C"`` into ``["A", "B", "C"]``; pass lists through untouched.

  Lists pass through because the same fields are also fed from TOML, where a
  list of symbols is written as a TOML array.
  """
  if isinstance(value, str):
    return [item.strip() for item in value.split(",") if item.strip()]
  return value


#: A comma-separated env var decoded into a list (``NoDecode`` stops
#: pydantic-settings from insisting on JSON for list fields).
CsvList = Annotated[list[str], NoDecode]


class MarketConfigError(Exception):
  """A market TOML file is missing, malformed or names something unknown.

  Lives here rather than in ``core/errors.py`` because the core imports these
  settings, and the import would run in a circle.
  """


class AppSettings(BaseSettings):
  """FastAPI process (env prefix ``APP_``)."""

  model_config = _config("APP_")

  NAME: str = "algo-trading-ingester"
  HOST: str = "0.0.0.0"
  PORT: int = 8090
  #: Identifies this process in events and notifications. Empty = hostname.
  INSTANCE_ID: str = ""
  DOCS_ENABLED: bool = False

  @property
  def instance_id(self) -> str:
    return self.INSTANCE_ID or socket.gethostname()


class SourceSettings(BaseSettings):
  """Which markets this process ingests, and where their TOML files live
  (env prefix ``SOURCE_``).

  ``SOURCE_MARKET=forex,crypto`` runs both: each market's gateways are read
  from ``<CONFIG_DIR>/<market>.toml`` and started side by side, so one venue
  going down never touches the other.
  """

  model_config = _config("SOURCE_")

  MARKET: Annotated[list[MarketEnum], NoDecode] = [MarketEnum.FOREX]
  CONFIG_DIR: Path = Path("config")

  @field_validator("MARKET", mode="before")
  @classmethod
  def _split_markets(cls, value: Any) -> Any:
    return [str(item).lower() for item in split_csv(value)]


class ContractSettings(BaseSettings):
  """Version stamped on every published payload (env prefix ``SCHEMA_``)."""

  model_config = _config("SCHEMA_")

  #: Bump on any breaking change to the payload shape. Subscribers should reject
  #: (or route aside) a major version they do not understand.
  VERSION: str = "1.0.0"


class LoggingSettings(BaseSettings):
  """Application logging (env prefix ``LOG_``)."""

  model_config = _config("LOG_")

  LEVEL: str = "INFO"
  DIR: str = "logs"


class NatsSettings(BaseSettings):
  """NATS — bars published one way, history served on request (prefix ``NATS_``).

  Two subject trees, kept apart on purpose:

  * ``<SUBJECT_PREFIX>.>`` — what the ingester publishes. With JetStream on, the
    stream captures all of it.
  * ``<rpc_prefix>.>`` — what the ingester is asked. Core NATS request/reply,
    never persisted. It must not sit under ``SUBJECT_PREFIX``: the stream would
    store every request as if it were market data, and answer it with its own
    acknowledgement before the ingester could.
  """

  model_config = _config("NATS_")

  HOST: str = "localhost"
  PORT: int = 4222
  TOKEN: str = ""  # token auth; blank = no auth
  #: First token of every subject: ``<PREFIX>.bar.closed.<gateway>.<symbol>.<tf>``.
  SUBJECT_PREFIX: str = "INGEST"
  CONNECT_TIMEOUT: float = 5.0
  RECONNECT_TIME_WAIT: float = 2.0
  #: Failed connection attempts inside ``GIVE_UP_WINDOW_SECONDS`` after which
  #: the ingester stops itself instead of retrying forever. An ingester that
  #: cannot reach NATS publishes nothing, so retrying for hours only hides the
  #: outage; it shuts down, says why on Telegram and waits for an operator.
  #: One attempt is counted per ``RECONNECT_TIME_WAIT`` while the link is down,
  #: which is nats-py's own retry cadence — at the defaults, 300 attempts is
  #: ten minutes of being unreachable inside any half-hour. 0 disables it and
  #: the process retries for as long as it runs.
  GIVE_UP_AFTER_ATTEMPTS: int = Field(default=300, ge=0)
  #: Length of the rolling window those attempts are counted in. Attempts are
  #: never reset by a successful reconnect, so a link that keeps flapping
  #: inside the window gives up too.
  GIVE_UP_WINDOW_SECONDS: float = Field(default=1800.0, gt=0)
  #: Core NATS publish is fire-and-forget; JetStream persists each bar so a
  #: subscriber that was down can replay it, de-duplicated by ``event_id``.
  JETSTREAM_ENABLED: bool = False
  PUBLISH_TIMEOUT: float = 5.0
  #: How long the stream keeps a bar (only used when the stream is created).
  STREAM_MAX_AGE_SECONDS: float = 7 * 24 * 3600
  DUPLICATE_WINDOW_SECONDS: float = 120.0
  #: First token of every request subject:
  #: ``<prefix>.history.<gateway>.<symbol>.<timeframe>``. Blank derives it from
  #: ``SUBJECT_PREFIX`` (``INGESTER`` → ``INGESTER_RPC``), which is what the
  #: subscriber assumes too.
  RPC_SUBJECT_PREFIX: str = ""
  #: Most bars one history request is answered with, whatever it asks for. The
  #: reply is one NATS message, so this is also bounded by the server's
  #: ``max_payload``; a reply over either limit keeps the newest bars and says
  #: ``truncated``.
  HISTORY_MAX_BARS: int = Field(default=5000, ge=1)
  #: Seconds a gateway may take to read the bars for one request before the
  #: caller is told ``timeout``.
  HISTORY_TIMEOUT: float = Field(default=20.0, gt=0)

  @model_validator(mode="after")
  def _keep_requests_out_of_the_stream(self) -> NatsSettings:
    publish_prefix = self.SUBJECT_PREFIX
    if self.rpc_prefix == publish_prefix or self.rpc_prefix.startswith(
      f"{publish_prefix}."
    ):
      raise ValueError(
        f"NATS_RPC_SUBJECT_PREFIX {self.rpc_prefix!r} sits under "
        f"NATS_SUBJECT_PREFIX {publish_prefix!r}: the JetStream stream listens on "
        f"{publish_prefix}.> and would capture every request. Use a prefix of "
        "its own, or leave it blank."
      )
    return self

  @property
  def url(self) -> str:
    return f"nats://{self.HOST}:{self.PORT}"

  @property
  def rpc_prefix(self) -> str:
    """The request prefix: configured, or ``<stream name>_RPC``."""
    return self.RPC_SUBJECT_PREFIX.strip(".") or f"{self.stream_name}_RPC"

  @property
  def stream_name(self) -> str:
    """JetStream stream name — derived from ``SUBJECT_PREFIX``, never set.

    The stream has to listen on ``<SUBJECT_PREFIX>.>`` to accept what this
    ingester publishes, and a stream is never reconfigured once it exists. A
    separately configured name is therefore a silent outage waiting to happen:
    change one and not the other and every publish is rejected. Deriving the
    name removes the chance to get them out of step, at the cost of not being
    able to version the stream independently of the subject tree. Only the
    prefix's first token is used, because a stream name cannot hold a dot.
    """
    return _STREAM_NAME_UNSAFE.sub("_", self.SUBJECT_PREFIX.split(".", 1)[0])


class TelegramSettings(BaseSettings):
  """Telegram service-status notifications (env prefix ``TELEGRAM_``)."""

  model_config = _config("TELEGRAM_")

  ENABLED: bool = False
  BOT_TOKEN: str = ""
  #: Comma-separated chat ids; ``<chat>_<topic>`` targets a forum topic.
  CHAT_IDS: str = ""
  HTTP_TIMEOUT: float = 5.0
  #: Bounded so a Telegram outage can never grow memory without limit.
  QUEUE_SIZE: int = 100
  #: Mirror every ERROR-level log record into its own chat, so failures are not
  #: buried between the lifecycle messages.
  LOG_ERRORS_ENABLED: bool = False
  #: Both fall back to BOT_TOKEN / CHAT_IDS when blank.
  LOG_BOT_TOKEN: str = ""
  LOG_CHAT_IDS: str = ""
  #: Seconds an identical error is suppressed — a failing poll repeats every
  #: interval and would otherwise flood the chat.
  LOG_DEDUP_WINDOW: float = 60.0

  @property
  def log_bot_token(self) -> str:
    return self.LOG_BOT_TOKEN or self.BOT_TOKEN

  @property
  def log_chat_ids(self) -> str:
    return self.LOG_CHAT_IDS or self.CHAT_IDS


class GatewaySettings(BaseSettings):
  """What every gateway shares, read from its ``[gateway]`` table in a market
  file.

  Subclass per gateway with its own ``env_prefix`` — the prefix only serves the
  fields a venue keeps in ``.env`` (credentials, local paths); everything else
  arrives as init arguments from the TOML table, which take precedence.
  """

  #: Fields a market file may **not** set, lower-cased in the error. ``MARKET``
  #: is the file's own name, so a table claiming another one could only
  #: contradict it; a subclass adds its secrets and host paths, which belong in
  #: ``.env`` and must not be invited into a file meant to be read and diffed.
  ENV_ONLY_FIELDS: ClassVar[frozenset[str]] = frozenset({"MARKET"})

  #: First key of every table: whether this market starts this gateway.
  ENABLE: bool = False
  SYMBOLS: CsvList = []
  TIMEFRAMES: Annotated[list[Timeframe], NoDecode] = [Timeframe.M1]
  #: Set from the name of the market file the table was read from.
  MARKET: MarketEnum

  @field_validator("SYMBOLS", mode="before")
  @classmethod
  def _split_symbols(cls, value: Any) -> Any:
    return split_csv(value)

  @field_validator("TIMEFRAMES", mode="before")
  @classmethod
  def _split_timeframes(cls, value: Any) -> Any:
    return [str(item).upper() for item in split_csv(value)]


class Mt5Settings(GatewaySettings):
  """MetaTrader 5 terminal bridge — ``[mt5]`` in a market file, credentials
  from ``.env`` (prefix ``MT5_``)."""

  model_config = _config("MT5_")

  ENV_ONLY_FIELDS: ClassVar[frozenset[str]] = GatewaySettings.ENV_ONLY_FIELDS | {
    "TERMINAL_PATH",
    "LOGIN",
    "PASSWORD",
    "SERVER",
    "TIMEOUT_MS",
  }

  MARKET: MarketEnum = MarketEnum.FOREX
  #: Forces the broker's affix when auto-detection is ambiguous (Exness: ``m``,
  #: so ``XAUUSD`` is read from ``XAUUSDm``). Blank = detect it per symbol.
  SYMBOL_SUFFIX: str = ""
  #: terminal64.exe path; blank = the terminal the MetaTrader5 package finds.
  TERMINAL_PATH: str = ""
  #: Leave LOGIN blank to attach to whichever account the terminal is logged in.
  LOGIN: int | None = None
  PASSWORD: str = ""
  SERVER: str = ""
  TIMEOUT_MS: int = 60_000
  #: MT5 stamps bars in *trade-server* time, not UTC. Name the server's IANA
  #: zone (e.g. ``Europe/Athens`` for the common GMT+2/+3 DST servers) so bar
  #: times are converted correctly.
  SERVER_TIMEZONE: str = "UTC"
  #: How often the watcher thread checks for a newly closed bar.
  POLL_INTERVAL_SECONDS: float = 1.0
  #: Closed bars read per check — how many bars a terminal outage can recover
  #: once it reconnects. It has nothing to do with a subscriber's indicator
  #: window: that is asked for, in whatever size the subscriber wants.
  RECOVERY_BARS: int = Field(default=5, ge=1)
  RECONNECT_INTERVAL_SECONDS: float = 5.0

  @field_validator("LOGIN", mode="before")
  @classmethod
  def _blank_login(cls, value: Any) -> Any:
    return None if value in ("", None) else value


class BinanceSettings(GatewaySettings):
  """Binance kline websocket — ``[binance]`` in a market file (prefix
  ``BINANCE_``).

  Binance pushes a kline update several times a second and marks the final one
  ``x: true``; only that one is a closed bar, so no polling interval is needed.
  """

  model_config = _config("BINANCE_")

  MARKET: MarketEnum = MarketEnum.CRYPTO
  #: Combined-stream endpoint. ``wss://testnet.binance.vision/stream`` for the
  #: testnet; ``wss://fstream.binance.com/stream`` for USD-M futures.
  WS_URL: str = "wss://stream.binance.com:9443/stream"
  #: The full REST klines endpoint a history request is answered from — the
  #: websocket only carries bars that close while it is connected. Its path
  #: differs per product: ``https://testnet.binance.vision/api/v3/klines`` for the
  #: testnet, ``https://fapi.binance.com/fapi/v1/klines`` for USD-M futures.
  #: Keep it on the same product as ``WS_URL`` or the two disagree about which
  #: book the bars came from.
  KLINES_URL: str = "https://api.binance.com/api/v3/klines"
  #: Seconds a single klines request may take.
  HTTP_TIMEOUT_SECONDS: float = 10.0
  #: Seconds before a dropped socket is dialled again.
  RECONNECT_INTERVAL_SECONDS: float = 5.0
  #: Websocket keep-alive. Binance answers pings; a silent peer is dropped
  #: after PING_TIMEOUT so the reconnect loop can take over.
  PING_INTERVAL_SECONDS: float = 20.0
  PING_TIMEOUT_SECONDS: float = 20.0
  #: Seconds without any message before the connection counts as dead. Binance
  #: pushes kline updates continuously, so silence means the socket is a husk.
  IDLE_TIMEOUT_SECONDS: float = 90.0


#: Gateway → the settings class that validates its ``[gateway]`` table. A new
#: venue adds its class above and one row here; nothing else parses config.
GATEWAY_SETTINGS: dict[GatewayEnum, type[GatewaySettings]] = {
  GatewayEnum.MT5: Mt5Settings,
  GatewayEnum.BINANCE: BinanceSettings,
}


@dataclass(frozen=True)
class MarketSettings:
  """One market file: the gateways it enables, in the order they are written.

  A plain dataclass, not a pydantic model: the values are ``GatewaySettings``
  *subclasses* and pydantic would validate them back down to the base class,
  dropping every venue-specific field.
  """

  market: MarketEnum
  gateways: dict[GatewayEnum, GatewaySettings] = field(default_factory=dict)


def market_config_path(market: MarketEnum, config_dir: Path) -> Path:
  return config_dir / f"{market.value}.toml"


def load_market(market: MarketEnum, config_dir: Path) -> MarketSettings:
  """Read ``<config_dir>/<market>.toml`` and return its **enabled** gateways.

  Raises :class:`MarketConfigError` for anything an operator can fix: a missing
  file, broken TOML, a table that is not a known gateway, a mistyped key or a
  value the gateway's settings reject. Loud beats a market that silently
  ingests nothing.
  """
  path = market_config_path(market, config_dir)
  try:
    with path.open("rb") as handle:
      tables = tomllib.load(handle)
  except FileNotFoundError:
    example = path.with_suffix(".example.toml")
    # Only point at a template that is actually there: SOURCE_CONFIG_DIR can
    # name a directory the committed templates were never copied to.
    hint = f"copy {example} to {path}" if example.is_file() else f"create {path}"
    raise MarketConfigError(
      f"market {market.value!r}: {path} is missing ({hint})"
    ) from None
  except (OSError, tomllib.TOMLDecodeError) as exc:
    raise MarketConfigError(
      f"market {market.value!r}: cannot read {path}: {exc}"
    ) from None

  gateways: dict[GatewayEnum, GatewaySettings] = {}
  for name, table in tables.items():
    if not isinstance(table, dict):
      raise MarketConfigError(
        f"{path}: {name!r} must be a gateway table, written as [{name}]"
      )
    try:
      gateway = GatewayEnum(name.lower())
    except ValueError:
      known = ", ".join(g.value for g in GATEWAY_SETTINGS)
      raise MarketConfigError(
        f"{path}: unknown gateway table [{name}] (known gateways: {known})"
      ) from None

    settings_class = GATEWAY_SETTINGS[gateway]
    # TOML is written in lower case; the settings fields are upper case. The
    # market is not in the table at all — it is the file this table lives in.
    values = {key.upper(): value for key, value in table.items()}
    unknown = sorted(
      key.lower() for key in values if key not in settings_class.model_fields
    )
    if unknown:
      raise MarketConfigError(
        f"{path}: [{name}] has unknown key(s): {', '.join(unknown)}"
      )
    # Accepting these from a market file would mean a table that contradicts
    # its own file, or a secret written into a diffable file — so they are
    # refused rather than quietly overwritten.
    refused = sorted(
      key.lower() for key in values if key in settings_class.ENV_ONLY_FIELDS
    )
    if refused:
      raise MarketConfigError(
        f"{path}: [{name}] must not set {', '.join(refused)} — the market is "
        f"this file's name and the rest belongs in .env as "
        f"{name.upper()}_<KEY>"
      )
    values["MARKET"] = market
    try:
      config = settings_class(**values)
    except ValidationError as exc:
      raise MarketConfigError(f"{path}: [{name}] is invalid: {exc}") from None

    if config.ENABLE:
      gateways[gateway] = config
  return MarketSettings(market=market, gateways=gateways)


def load_markets(
  markets: list[MarketEnum], config_dir: Path
) -> tuple[dict[MarketEnum, MarketSettings], list[str]]:
  """Load every market, collecting failures instead of raising.

  One unreadable market file must not stop the markets that *are* configured:
  the runtime starts what loaded and reports the rest as **Ingester Degraded**.

  A gateway enabled in two markets is refused in the second: venue SDKs are
  process-global (one ``MetaTrader5`` session, which either ingestion's
  ``shutdown()`` would pull out from under the other) and ``event_id`` carries
  no market, so two instances of one gateway would publish colliding ids.
  """
  loaded: dict[MarketEnum, MarketSettings] = {}
  problems: list[str] = []
  claimed: dict[GatewayEnum, MarketEnum] = {}
  for market in dict.fromkeys(markets):
    try:
      market_settings = load_market(market, config_dir)
    except MarketConfigError as exc:
      problems.append(str(exc))
      continue
    gateways: dict[GatewayEnum, GatewaySettings] = {}
    for gateway, config in market_settings.gateways.items():
      owner = claimed.get(gateway)
      if owner is not None:
        problems.append(
          f"gateway {gateway.value!r} is enabled in both {owner.value!r} and "
          f"{market.value!r}; only the {owner.value!r} one runs — set "
          f"enable = false in {market_config_path(market, config_dir)}"
        )
        continue
      claimed[gateway] = market
      gateways[gateway] = config
    loaded[market] = MarketSettings(market=market, gateways=gateways)
  return loaded, problems


class Settings(BaseSettings):
  """Application-wide configuration, grouped into nested ``*Settings`` models."""

  model_config = SettingsConfigDict(
    env_file=".env",
    env_file_encoding="utf-8",
    extra="ignore",
  )

  app: AppSettings = Field(default_factory=AppSettings)
  source: SourceSettings = Field(default_factory=SourceSettings)
  contract: ContractSettings = Field(default_factory=ContractSettings)
  logging: LoggingSettings = Field(default_factory=LoggingSettings)
  nats: NatsSettings = Field(default_factory=NatsSettings)
  telegram: TelegramSettings = Field(default_factory=TelegramSettings)
  #: Market → its enabled gateways, read from the market TOML files. Skipping
  #: validation keeps the ``GatewaySettings`` subclasses intact; tests pass
  #: their own markets in and no file is read.
  markets: SkipValidation[dict[MarketEnum, MarketSettings] | None] = None
  #: One line per market that could not be loaded, reported at start-up.
  market_problems: SkipValidation[list[str]] = Field(default_factory=list)

  def model_post_init(self, _context: Any) -> None:
    if self.markets is None:
      markets, problems = load_markets(self.source.MARKET, self.source.CONFIG_DIR)
      self.markets = markets
      self.market_problems = problems

  @property
  def gateway_configs(self) -> list[tuple[MarketEnum, GatewayEnum, GatewaySettings]]:
    """Every enabled gateway across every loaded market, in configured order."""
    return [
      (market.market, gateway, config)
      for market in (self.markets or {}).values()
      for gateway, config in market.gateways.items()
    ]


settings = Settings()
