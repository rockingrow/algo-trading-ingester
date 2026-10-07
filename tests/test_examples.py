"""The committed example payloads are the contract ``qte-ingest`` codes against.

The committed ``config/*.example.toml`` templates are the only configuration in
the repository — an operator's own ``config/*.toml`` is git-ignored — so a typo
in a template would otherwise surface in production.
"""

import json
from pathlib import Path

import pytest

from ingester.schemas import (
  BarClosedEvent,
  GatewayEnum,
  HistoryReply,
  HistoryRequest,
  MarketEnum,
  OnlineAnnouncement,
)
from ingester.settings import MarketConfigError, load_market

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "examples" / "nats"
CONFIG = ROOT / "config"


@pytest.mark.parametrize(
  "name",
  [
    "bar.closed.mt5.json",
    "bar.closed.binance.json",
  ],
)
def test_bar_closed_example_matches_schema(name):
  raw = json.loads((EXAMPLES / name).read_text(encoding="utf-8"))
  event = BarClosedEvent.model_validate(raw)
  assert event.model_dump(mode="json") == raw


def test_history_request_example_matches_schema():
  raw = json.loads((EXAMPLES / "history.request.mt5.json").read_text(encoding="utf-8"))
  assert HistoryRequest.model_validate(raw).model_dump(mode="json") == raw


def test_online_announcement_example_matches_schema():
  raw = json.loads((EXAMPLES / "ingester.online.mt5.json").read_text(encoding="utf-8"))
  assert OnlineAnnouncement.model_validate(raw).model_dump(mode="json") == raw


@pytest.mark.parametrize("name", ["history.reply.mt5.json", "history.reply.error.json"])
def test_history_reply_example_matches_schema(name):
  raw = json.loads((EXAMPLES / name).read_text(encoding="utf-8"))
  assert HistoryReply.model_validate(raw).model_dump(mode="json") == raw


@pytest.mark.parametrize(
  ("market", "gateway"),
  [(MarketEnum.FOREX, GatewayEnum.MT5), (MarketEnum.CRYPTO, GatewayEnum.BINANCE)],
)
def test_market_template_loads(market, gateway, tmp_path):
  # load_market() reads "<market>.toml"; the template is what an operator
  # copies to that name.
  template = CONFIG / f"{market.value}.example.toml"
  (tmp_path / f"{market.value}.toml").write_text(
    template.read_text(encoding="utf-8"), encoding="utf-8"
  )
  loaded = load_market(market, tmp_path)
  config = loaded.gateways[gateway]
  assert config.MARKET is market
  assert config.SYMBOLS and config.TIMEFRAMES


def test_the_example_itself_is_not_what_gets_loaded(tmp_path):
  # SOURCE_MARKET=forex looks for config/forex.toml, never the template: the
  # error has to point the operator at the copy they are missing.
  (tmp_path / "forex.example.toml").write_text(
    (CONFIG / "forex.example.toml").read_text(encoding="utf-8"), encoding="utf-8"
  )
  with pytest.raises(MarketConfigError, match="copy .*forex.example.toml"):
    load_market(MarketEnum.FOREX, tmp_path)
