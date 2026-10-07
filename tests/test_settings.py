import pytest
from pydantic import ValidationError

from ingester.schemas import GatewayEnum, MarketEnum, Timeframe
from ingester.settings import (
  AppSettings,
  BinanceSettings,
  MarketConfigError,
  Mt5Settings,
  NatsSettings,
  Settings,
  SourceSettings,
  load_market,
  load_markets,
)

FOREX_TOML = """
[mt5]
enable = true
symbols = ["XAUUSD", "EURUSD"]
timeframes = ["m1", "M15"]
server_timezone = "Europe/Athens"
recovery_bars = 16
"""

CRYPTO_TOML = """
[binance]
enable = true
symbols = ["BTCUSDT"]
timeframes = ["H1"]
http_timeout_seconds = 3.0
"""


def write(tmp_path, name: str, body: str):
  path = tmp_path / name
  path.write_text(body, encoding="utf-8")
  return path


def test_csv_env_vars_are_parsed(monkeypatch):
  monkeypatch.setenv("MT5_SYMBOLS", " XAUUSD, EURUSD ,,BTCUSD ")
  monkeypatch.setenv("MT5_TIMEFRAMES", "m1,M15, h1")
  monkeypatch.setenv("MT5_LOGIN", "")
  config = Mt5Settings(_env_file=None)
  assert config.SYMBOLS == ["XAUUSD", "EURUSD", "BTCUSD"]
  assert config.TIMEFRAMES == [Timeframe.M1, Timeframe.M15, Timeframe.H1]
  assert config.LOGIN is None


def test_instance_id_and_port(monkeypatch):
  monkeypatch.setenv("APP_INSTANCE_ID", "vps-01")
  monkeypatch.setenv("APP_PORT", "9000")
  config = AppSettings(_env_file=None)
  assert config.instance_id == "vps-01"
  assert config.PORT == 9000


def test_instance_id_defaults_to_hostname(monkeypatch):
  import socket

  monkeypatch.delenv("APP_INSTANCE_ID", raising=False)
  assert AppSettings(_env_file=None).instance_id == socket.gethostname()


def test_nats_url(monkeypatch):
  monkeypatch.setenv("NATS_HOST", "nats.internal")
  monkeypatch.setenv("NATS_PORT", "4333")
  assert NatsSettings(_env_file=None).url == "nats://nats.internal:4333"


def test_stream_name_is_derived_from_the_subject_prefix(monkeypatch):
  monkeypatch.setenv("NATS_SUBJECT_PREFIX", "INGEST")
  assert NatsSettings(_env_file=None).stream_name == "INGEST"


def test_stream_name_keeps_only_the_prefix_first_token(monkeypatch):
  # A stream name cannot hold a dot, so a multi-token prefix is truncated
  # rather than rejected by the server on the first publish.
  monkeypatch.setenv("NATS_SUBJECT_PREFIX", "INGEST.prod")
  assert NatsSettings(_env_file=None).stream_name == "INGEST"


def test_stream_name_replaces_characters_a_stream_cannot_hold(monkeypatch):
  monkeypatch.setenv("NATS_SUBJECT_PREFIX", "my/prefix")
  assert NatsSettings(_env_file=None).stream_name == "my_prefix"


def test_stream_name_ignores_a_leftover_stream_name_variable(monkeypatch):
  # The setting is gone; an old .env line must not resurrect it.
  monkeypatch.setenv("NATS_SUBJECT_PREFIX", "INGEST")
  monkeypatch.setenv("NATS_STREAM_NAME", "STALE")
  assert NatsSettings(_env_file=None).stream_name == "INGEST"


def test_source_market_is_a_csv_list(monkeypatch):
  monkeypatch.setenv("SOURCE_MARKET", "FOREX, crypto ,,forex")
  config = SourceSettings(_env_file=None)
  assert config.MARKET == [MarketEnum.FOREX, MarketEnum.CRYPTO, MarketEnum.FOREX]


# ── Market TOML files ───────────────────────────────────────────────


def test_market_file_fills_the_gateway_settings(tmp_path):
  write(tmp_path, "forex.toml", FOREX_TOML)
  market = load_market(MarketEnum.FOREX, tmp_path)

  assert market.market is MarketEnum.FOREX
  config = market.gateways[GatewayEnum.MT5]
  assert isinstance(config, Mt5Settings)
  assert config.SYMBOLS == ["XAUUSD", "EURUSD"]
  assert config.TIMEFRAMES == [Timeframe.M1, Timeframe.M15]
  assert config.SERVER_TIMEZONE == "Europe/Athens"
  assert config.RECOVERY_BARS == 16
  # The market is the file's name, never repeated inside the table.
  assert config.MARKET is MarketEnum.FOREX


def test_crypto_file_yields_binance_settings(tmp_path):
  write(tmp_path, "crypto.toml", CRYPTO_TOML)
  market = load_market(MarketEnum.CRYPTO, tmp_path)
  config = market.gateways[GatewayEnum.BINANCE]
  assert isinstance(config, BinanceSettings)
  assert config.MARKET is MarketEnum.CRYPTO
  assert config.SYMBOLS == ["BTCUSDT"]
  assert config.TIMEFRAMES == [Timeframe.H1]
  assert config.HTTP_TIMEOUT_SECONDS == 3.0


@pytest.mark.parametrize(
  ("table", "key"),
  [
    ("mt5", "warmup_bars = 150"),
    ("mt5", "backfill_on_start = true"),
    ("binance", "warmup_bars = 16"),
    ("binance", "backfill_on_start = true"),
  ],
)
def test_the_retired_warmup_keys_are_refused_by_name(tmp_path, table, key):
  # The subscriber decides what to warm and how much; a market file still
  # carrying these would otherwise look as though it did.
  market = MarketEnum.FOREX if table == "mt5" else MarketEnum.CRYPTO
  write(tmp_path, f"{market.value}.toml", f"[{table}]\nenable = true\n{key}\n")
  with pytest.raises(MarketConfigError, match="unknown key"):
    load_market(market, tmp_path)


def test_rpc_prefix_is_derived_from_the_subject_prefix():
  assert NatsSettings(_env_file=None, SUBJECT_PREFIX="INGESTER").rpc_prefix == (
    "INGESTER_RPC"
  )
  assert (
    NatsSettings(_env_file=None, SUBJECT_PREFIX="DESK.MT5").rpc_prefix == "DESK_RPC"
  )
  assert NatsSettings(_env_file=None, RPC_SUBJECT_PREFIX="ASK.").rpc_prefix == "ASK"


@pytest.mark.parametrize("rpc_prefix", ["INGESTER", "INGESTER.rpc", "INGESTER.rpc.v1"])
def test_an_rpc_prefix_inside_the_published_tree_is_refused(rpc_prefix):
  # The stream listens on INGESTER.> — it would store every request and answer
  # it with its own acknowledgement before the ingester could.
  with pytest.raises(ValidationError, match="sits under"):
    NatsSettings(
      _env_file=None, SUBJECT_PREFIX="INGESTER", RPC_SUBJECT_PREFIX=rpc_prefix
    )


def test_env_fills_what_the_table_leaves_out(tmp_path, monkeypatch):
  # Credentials stay in .env; the table owns what to ingest. Init arguments
  # outrank the environment, so the table wins where both speak.
  monkeypatch.setenv("MT5_LOGIN", "12345")
  monkeypatch.setenv("MT5_PASSWORD", "from-env")
  monkeypatch.setenv("MT5_SERVER_TIMEZONE", "UTC")
  write(tmp_path, "forex.toml", FOREX_TOML)
  config = load_market(MarketEnum.FOREX, tmp_path).gateways[GatewayEnum.MT5]
  assert config.LOGIN == 12345
  assert config.PASSWORD == "from-env"
  assert config.SERVER_TIMEZONE == "Europe/Athens"


def test_disabled_gateway_is_left_out(tmp_path):
  write(tmp_path, "forex.toml", FOREX_TOML.replace("enable = true", "enable = false"))
  assert load_market(MarketEnum.FOREX, tmp_path).gateways == {}


def test_a_table_without_enable_does_not_run(tmp_path):
  # Opting in has to be explicit: a half-written table must not start trading
  # data flowing on its own.
  write(tmp_path, "forex.toml", FOREX_TOML.replace("enable = true\n", ""))
  assert load_market(MarketEnum.FOREX, tmp_path).gateways == {}


def test_missing_file_names_the_template(tmp_path):
  # The template is there; the copy an operator has to make is not.
  write(tmp_path, "forex.example.toml", FOREX_TOML)
  with pytest.raises(MarketConfigError, match="copy .*forex.example.toml"):
    load_market(MarketEnum.FOREX, tmp_path)


def test_broken_toml_is_reported(tmp_path):
  write(tmp_path, "forex.toml", "[mt5\nenable = true")
  with pytest.raises(MarketConfigError, match="cannot read"):
    load_market(MarketEnum.FOREX, tmp_path)


def test_unknown_gateway_table_is_reported(tmp_path):
  write(tmp_path, "forex.toml", "[kraken]\nenable = true\n")
  with pytest.raises(MarketConfigError, match="unknown gateway table"):
    load_market(MarketEnum.FOREX, tmp_path)


def test_mistyped_key_is_reported(tmp_path):
  # Silently ignoring it would leave the gateway polling at its default and the
  # operator convinced they had changed it.
  write(tmp_path, "forex.toml", '[mt5]\nenable = true\nsymbol = ["XAUUSD"]\n')
  with pytest.raises(MarketConfigError, match="unknown key"):
    load_market(MarketEnum.FOREX, tmp_path)


def test_invalid_value_is_reported(tmp_path):
  write(tmp_path, "forex.toml", "[mt5]\nenable = true\nrecovery_bars = 0\n")
  with pytest.raises(MarketConfigError, match="invalid"):
    load_market(MarketEnum.FOREX, tmp_path)


def test_a_bare_key_outside_a_table_is_reported(tmp_path):
  write(tmp_path, "forex.toml", 'symbols = ["XAUUSD"]\n')
  with pytest.raises(MarketConfigError, match="gateway table"):
    load_market(MarketEnum.FOREX, tmp_path)


def test_one_bad_market_does_not_stop_the_others(tmp_path):
  # The runtime reports it as degraded and still ingests crypto.
  write(tmp_path, "crypto.toml", CRYPTO_TOML)
  loaded, problems = load_markets([MarketEnum.FOREX, MarketEnum.CRYPTO], tmp_path)
  assert list(loaded) == [MarketEnum.CRYPTO]
  assert len(problems) == 1
  assert "forex" in problems[0]


def test_settings_loads_every_configured_market(tmp_path, monkeypatch):
  write(tmp_path, "forex.toml", FOREX_TOML)
  write(tmp_path, "crypto.toml", CRYPTO_TOML)
  monkeypatch.setenv("SOURCE_MARKET", "forex,crypto")
  monkeypatch.setenv("SOURCE_CONFIG_DIR", str(tmp_path))

  config = Settings(_env_file=None)
  assert config.market_problems == []
  assert [(m.value, g.value) for m, g, _ in config.gateway_configs] == [
    ("forex", "mt5"),
    ("crypto", "binance"),
  ]


def test_injected_markets_read_no_files(tmp_path, monkeypatch):
  monkeypatch.setenv("SOURCE_CONFIG_DIR", str(tmp_path))
  config = Settings(_env_file=None, markets={})
  assert config.markets == {}
  assert config.market_problems == []


def test_the_market_cannot_be_overridden_in_a_table(tmp_path):
  # A [mt5] table in forex.toml claiming market = "crypto" could only
  # contradict the file it lives in; silently winning would publish forex bars
  # labelled crypto.
  write(tmp_path, "forex.toml", FOREX_TOML + '\nmarket = "crypto"\n')
  with pytest.raises(MarketConfigError, match="must not set market"):
    load_market(MarketEnum.FOREX, tmp_path)


@pytest.mark.parametrize("key", ["login", "password", "server", "terminal_path"])
def test_secrets_are_refused_in_a_market_file(key, tmp_path):
  # config/*.toml is meant to be read and diffed; credentials stay in .env.
  write(tmp_path, "forex.toml", f'[mt5]\nenable = true\n{key} = "x"\n')
  with pytest.raises(MarketConfigError, match=f"must not set {key}"):
    load_market(MarketEnum.FOREX, tmp_path)


def test_one_gateway_cannot_run_in_two_markets(tmp_path):
  # MetaTrader5 keeps one process-global session, so a second Mt5Ingestion
  # would shut the first one's terminal down — and both would publish the same
  # event_id, which carries no market.
  write(tmp_path, "forex.toml", FOREX_TOML)
  write(tmp_path, "crypto.toml", FOREX_TOML)
  loaded, problems = load_markets([MarketEnum.FOREX, MarketEnum.CRYPTO], tmp_path)

  assert list(loaded[MarketEnum.FOREX].gateways) == [GatewayEnum.MT5]
  assert loaded[MarketEnum.CRYPTO].gateways == {}
  assert len(problems) == 1
  assert "enabled in both 'forex' and 'crypto'" in problems[0]


def test_a_missing_market_without_a_template_says_create_it(tmp_path):
  # No template sits next to the missing file here; the error must not point
  # at one that does not exist.
  with pytest.raises(MarketConfigError, match="create"):
    load_market(MarketEnum.CRYPTO, tmp_path)


def test_cfd_is_not_a_market():
  # Only the markets with a gateway behind them are accepted.
  with pytest.raises(ValidationError):
    SourceSettings(_env_file=None, MARKET="forex,cfd")
  assert {market.value for market in MarketEnum} == {"forex", "crypto"}
