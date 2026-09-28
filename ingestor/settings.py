"""
ingestor/settings.py — Centralised settings loaded from .env / environment variables.

Settings are grouped into focused ``*Settings`` sub-models nested under the main
:class:`Settings` (``settings.app.PORT``, ``settings.nats.url``,
``settings.mt5.SYMBOLS``). Each sub-model carries an ``env_prefix`` so the env
var names stay flat — ``MT5_SYMBOLS`` populates ``settings.mt5.SYMBOLS`` — and
can be instantiated on its own (handy for tests).

Nothing about *what* to ingest is hard-coded: symbols, timeframes, the enabled
gateways, host/port and every NATS/Telegram knob come from ``.env``.
"""

from __future__ import annotations

import socket
from typing import Annotated, Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from ingestor.schemas.enums import GatewayEnum, MarketEnum, Timeframe


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
  """Turn ``"A, B,,C"`` into ``["A", "B", "C"]``; pass lists through untouched."""
  if isinstance(value, str):
    return [item.strip() for item in value.split(",") if item.strip()]
  return value


#: A comma-separated env var decoded into a list (``NoDecode`` stops
#: pydantic-settings from insisting on JSON for list fields).
CsvList = Annotated[list[str], NoDecode]


class AppSettings(BaseSettings):
  """FastAPI process (env prefix ``APP_``)."""

  model_config = _config("APP_")

  NAME: str = "algo-trading-ingestor"
  HOST: str = "0.0.0.0"
  PORT: int = 8090
  #: Identifies this process in events and notifications. Empty = hostname.
  INSTANCE_ID: str = ""
  #: Gateways started at boot, e.g. ``mt5`` or ``mt5,binance``.
  GATEWAYS: Annotated[list[GatewayEnum], NoDecode] = [GatewayEnum.MT5]
  DOCS_ENABLED: bool = False

  @field_validator("GATEWAYS", mode="before")
  @classmethod
  def _split_gateways(cls, value: Any) -> Any:
    return [str(item).lower() for item in split_csv(value)]

  @property
  def instance_id(self) -> str:
    return self.INSTANCE_ID or socket.gethostname()


class LoggingSettings(BaseSettings):
  """Application logging (env prefix ``LOG_``)."""

  model_config = _config("LOG_")

  LEVEL: str = "INFO"
  DIR: str = "logs"


class NatsSettings(BaseSettings):
  """NATS publishing — one-way, the ingestor never subscribes (prefix ``NATS_``)."""

  model_config = _config("NATS_")

  HOST: str = "localhost"
  PORT: int = 4222
  TOKEN: str = ""  # token auth; blank = no auth
  #: First token of every subject: ``<PREFIX>.bar.closed.<gateway>.<symbol>.<tf>``.
  SUBJECT_PREFIX: str = "INGEST"
  CONNECT_TIMEOUT: float = 5.0
  RECONNECT_TIME_WAIT: float = 2.0
  #: Core NATS publish is fire-and-forget; JetStream persists each bar so a
  #: subscriber that was down can replay it, de-duplicated by ``event_id``.
  JETSTREAM_ENABLED: bool = False
  STREAM_NAME: str = "INGEST"
  PUBLISH_TIMEOUT: float = 5.0
  #: How long the stream keeps a bar (only used when the stream is created).
  STREAM_MAX_AGE_SECONDS: float = 7 * 24 * 3600
  DUPLICATE_WINDOW_SECONDS: float = 120.0

  @property
  def url(self) -> str:
    return f"nats://{self.HOST}:{self.PORT}"


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


class GatewaySettings(BaseSettings):
  """What every gateway shares: which symbols and timeframes to ingest.

  Subclass per gateway with its own ``env_prefix`` and venue-specific knobs.
  """

  SYMBOLS: CsvList = []
  TIMEFRAMES: Annotated[list[Timeframe], NoDecode] = [Timeframe.M1]
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
  """MetaTrader 5 terminal bridge (env prefix ``MT5_``)."""

  model_config = _config("MT5_")

  MARKET: MarketEnum = MarketEnum.FOREX
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
  #: Closed bars fetched per check — how many bars a short outage can recover.
  CATCHUP_BARS: int = Field(default=5, ge=1)
  RECONNECT_INTERVAL_SECONDS: float = 5.0

  @field_validator("LOGIN", mode="before")
  @classmethod
  def _blank_login(cls, value: Any) -> Any:
    return None if value in ("", None) else value


class Settings(BaseSettings):
  """Application-wide configuration, grouped into nested ``*Settings`` models."""

  model_config = SettingsConfigDict(
    env_file=".env",
    env_file_encoding="utf-8",
    extra="ignore",
  )

  app: AppSettings = Field(default_factory=AppSettings)
  logging: LoggingSettings = Field(default_factory=LoggingSettings)
  nats: NatsSettings = Field(default_factory=NatsSettings)
  telegram: TelegramSettings = Field(default_factory=TelegramSettings)
  mt5: Mt5Settings = Field(default_factory=Mt5Settings)


settings = Settings()
