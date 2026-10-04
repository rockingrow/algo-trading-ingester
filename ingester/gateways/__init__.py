"""One sub-package per market, one sub-package per venue inside it.

``gateways/forex/mt5``, ``gateways/crypto/binance``: the folder a venue lives in
is the market it natively serves, which keeps the tree readable as venues are
added. It is a grouping only — the market a running gateway reports still comes
from the ``config/<market>.toml`` file its table was read from. Each venue owns
its DTO and its business logic, nothing else.
"""
