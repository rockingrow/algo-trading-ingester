import pytest

from ingester.core.errors import SymbolResolutionError
from ingester.gateways.mt5.symbols import resolve_symbol

# A slice of an Exness catalogue: the metal carries an "m", oil does not exist
# unsuffixed, and EURUSD is offered in two account flavours.
EXNESS = ["XAUUSDm", "USOILm", "EURUSDm", "EURUSDc", "GBPUSD", "XAUEURm"]


def test_exact_name_wins():
  assert resolve_symbol("XAUUSDm", EXNESS) == "XAUUSDm"


def test_suffix_is_detected():
  assert resolve_symbol("XAUUSD", EXNESS) == "XAUUSDm"
  assert resolve_symbol("USOIL", EXNESS) == "USOILm"


def test_unsuffixed_broker_is_passed_through():
  assert resolve_symbol("GBPUSD", EXNESS) == "GBPUSD"


def test_prefix_is_detected():
  assert resolve_symbol("XAUUSD", ["mXAUUSD"]) == "mXAUUSD"


def test_shortest_affix_wins():
  assert resolve_symbol("XAUUSD", ["XAUUSD.raw.ecn", "XAUUSDm"]) == "XAUUSDm"


def test_equally_close_candidates_are_ambiguous():
  with pytest.raises(SymbolResolutionError, match="ambiguous"):
    resolve_symbol("EURUSD", EXNESS)


def test_ambiguity_is_broken_by_configured_suffix():
  assert resolve_symbol("EURUSD", EXNESS, suffix="c") == "EURUSDc"


def test_configured_suffix_is_never_second_guessed():
  # "z" does not exist; resolving to XAUUSDm anyway would silently ingest the
  # wrong account's instrument.
  with pytest.raises(SymbolResolutionError, match="XAUUSDz"):
    resolve_symbol("XAUUSD", EXNESS, suffix="z")


def test_unknown_symbol_raises():
  with pytest.raises(SymbolResolutionError, match="no instrument"):
    resolve_symbol("NOPE", EXNESS)


def test_error_lists_candidates_for_the_operator():
  with pytest.raises(SymbolResolutionError) as excinfo:
    resolve_symbol("NOPE", ["XAUUSDm"])
  assert "XAUUSDm" in str(excinfo.value)
