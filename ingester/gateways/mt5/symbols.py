"""
ingester/gateways/mt5/symbols.py — Configured symbol → the broker's own name.

Brokers decorate instrument names: Exness sells XAUUSD as ``XAUUSDm``, others
use ``XAUUSD.raw``, ``XAUUSDc`` or a prefix. Naming the bare instrument in
The market file keeps configuration — and therefore every NATS subject — free of one
broker's spelling, so this module maps ``XAUUSD`` onto whatever the terminal
actually offers.

Only the MetaTrader5 calls use the resolved name. The published ``symbol`` stays
the configured one, so switching broker or account type never changes an
``event_id``.

Pure logic on purpose: it takes the candidate names as a list rather than a
terminal, so the whole matching table is testable without MetaTrader5.
"""

from __future__ import annotations

from collections.abc import Iterable

from ingester.core.errors import SymbolResolutionError


def resolve_symbol(base: str, candidates: Iterable[str], suffix: str = "") -> str:
  """Return the broker instrument that *base* refers to.

  Resolution order, first hit wins:

  1. *suffix* configured — ``base + suffix`` must exist, or it is an error. An
     explicit setting is never silently overruled by a guess.
  2. An exact match — so a fully-spelled ``XAUUSDm`` in the market file still
     works.
  3. The shortest name that adds an affix to *base*, suffix or prefix.

  Raises :class:`SymbolResolutionError` when nothing matches, or when several
  equally short candidates do and only an operator can pick.
  """
  names = list(dict.fromkeys(candidates))

  if suffix:
    explicit = f"{base}{suffix}"
    if explicit in names:
      return explicit
    raise SymbolResolutionError(
      f"symbol suffix {suffix!r} makes {base!r} into {explicit!r}, which this "
      f"broker does not offer (candidates: {_listed(names)})"
    )

  if base in names:
    return base

  affixed = [
    name
    for name in names
    if name != base and (name.startswith(base) or name.endswith(base))
  ]
  if not affixed:
    raise SymbolResolutionError(
      f"no instrument on this broker matches {base!r} (candidates: {_listed(names)})"
    )

  shortest = min(len(name) for name in affixed)
  best = sorted(name for name in affixed if len(name) == shortest)
  if len(best) > 1:
    raise SymbolResolutionError(
      f"{base!r} is ambiguous — {_listed(best)} are equally close; "
      f"set symbol_suffix in the [mt5] table or name the instrument in full"
    )
  return best[0]


def _listed(names: Iterable[str]) -> str:
  shown = sorted(names)[:10]
  return ", ".join(shown) if shown else "none"
