"""The committed example payloads are the contract ``qte-ingest`` codes against."""

import json
from pathlib import Path

from ingester.schemas import BarClosedEvent

EXAMPLES = Path(__file__).resolve().parents[1] / "examples" / "nats"


def test_bar_closed_example_matches_schema():
  raw = json.loads((EXAMPLES / "bar.closed.mt5.json").read_text(encoding="utf-8"))
  event = BarClosedEvent.model_validate(raw)
  assert event.model_dump(mode="json") == raw
