from ingester.schemas import GatewayEnum, MarketEnum, Timeframe
from ingester.settings import AppSettings, Mt5Settings, NatsSettings


def test_csv_env_vars_are_parsed(monkeypatch):
  monkeypatch.setenv("MT5_SYMBOLS", " XAUUSD, EURUSD ,,BTCUSD ")
  monkeypatch.setenv("MT5_TIMEFRAMES", "m1,M15, h1")
  monkeypatch.setenv("MT5_MARKET", "cfd")
  monkeypatch.setenv("MT5_LOGIN", "")
  config = Mt5Settings(_env_file=None)
  assert config.SYMBOLS == ["XAUUSD", "EURUSD", "BTCUSD"]
  assert config.TIMEFRAMES == [Timeframe.M1, Timeframe.M15, Timeframe.H1]
  assert config.MARKET is MarketEnum.CFD
  assert config.LOGIN is None


def test_app_gateways_and_instance_id(monkeypatch):
  monkeypatch.setenv("APP_GATEWAYS", "MT5")
  monkeypatch.setenv("APP_INSTANCE_ID", "vps-01")
  monkeypatch.setenv("APP_PORT", "9000")
  config = AppSettings(_env_file=None)
  assert config.GATEWAYS == [GatewayEnum.MT5]
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
