"""
ingestor/gateways/mt5/terminal.py — The seam between our code and MetaTrader5.

:class:`Mt5Terminal` is the small slice of the MetaTrader5 package the gateway
uses. :class:`MetaTrader5Terminal` implements it over the real package, which
only exists on Windows; tests implement it with a fake, so the business logic in
``ingestion.py`` runs anywhere.

Rates come back as plain ``dict``s of Python scalars rather than numpy records,
so nothing past this module needs numpy.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from ingestor.schemas.enums import Timeframe

RateRecord = Mapping[str, Any]


class Mt5Terminal(Protocol):
  """What the MT5 gateway needs from a terminal connection."""

  def initialize(
    self,
    *,
    path: str | None = None,
    login: int | None = None,
    password: str | None = None,
    server: str | None = None,
    timeout: int | None = None,
  ) -> bool: ...

  def shutdown(self) -> None: ...

  def last_error(self) -> tuple[int, str]: ...

  def is_connected(self) -> bool:
    """True while the terminal is up *and* connected to its trade server."""
    ...

  def server_name(self) -> str | None: ...

  def symbol_select(self, symbol: str, enable: bool = True) -> bool: ...

  def copy_rates_from_pos(
    self, symbol: str, timeframe: Timeframe, start_pos: int, count: int
  ) -> Sequence[RateRecord] | None: ...


class MetaTrader5Terminal:
  """:class:`Mt5Terminal` over the official ``MetaTrader5`` package."""

  def __init__(self) -> None:
    try:
      import MetaTrader5 as mt5  # Windows-only, so imported lazily
    except ImportError as exc:
      raise RuntimeError(
        "The MetaTrader5 package is not installed. It ships for Windows only — "
        "run the MT5 gateway on the Windows host that runs the terminal."
      ) from exc
    self._mt5 = mt5

  def initialize(
    self,
    *,
    path: str | None = None,
    login: int | None = None,
    password: str | None = None,
    server: str | None = None,
    timeout: int | None = None,
  ) -> bool:
    # MetaTrader5.initialize rejects explicit ``None`` for its keyword args, so
    # only the ones actually configured are passed.
    kwargs: dict[str, Any] = {
      key: value
      for key, value in {
        "login": login,
        "password": password,
        "server": server,
        "timeout": timeout,
      }.items()
      if value not in (None, "")
    }
    if path:
      return bool(self._mt5.initialize(path, **kwargs))
    return bool(self._mt5.initialize(**kwargs))

  def shutdown(self) -> None:
    self._mt5.shutdown()

  def last_error(self) -> tuple[int, str]:
    code, message = self._mt5.last_error()
    return int(code), str(message)

  def is_connected(self) -> bool:
    info = self._mt5.terminal_info()
    return info is not None and bool(info.connected)

  def server_name(self) -> str | None:
    account = self._mt5.account_info()
    return account.server if account is not None else None

  def symbol_select(self, symbol: str, enable: bool = True) -> bool:
    return bool(self._mt5.symbol_select(symbol, enable))

  def copy_rates_from_pos(
    self, symbol: str, timeframe: Timeframe, start_pos: int, count: int
  ) -> list[dict[str, Any]] | None:
    mt5_timeframe = getattr(self._mt5, f"TIMEFRAME_{timeframe.value}")
    rates = self._mt5.copy_rates_from_pos(symbol, mt5_timeframe, start_pos, count)
    if rates is None:
      return None
    names = rates.dtype.names
    return [{name: row[name].item() for name in names} for row in rates]
