# Changelog

## Unreleased

### Breaking — schema `2.0`

- Renamed `source.ingestor_id` to `source.ingester_id` in every published
  event. `qte-ingest` must read the new field name.
- `SCHEMA_VERSION` bumped from `1.0` to `2.0`.

### Added

- **Broker affix detection.** `MT5_SYMBOLS` names the bare instrument
  (`XAUUSD`); the gateway asks the terminal what this broker calls it
  (`XAUUSDm` on Exness) and reads bars from that name. The published `symbol`
  and therefore `event_id` stay the configured name, so the wire contract is
  unchanged and survives a change of broker. `MT5_SYMBOL_SUFFIX` settles a tie
  between equally close candidates; a fully spelled name is used verbatim.
- **`MT5_BACKFILL_ON_START`.** Publishes the closed bars found at start-up
  instead of only recording them, so a crash no longer silently skips the bars
  that closed while the process was down. Bounded by `MT5_CATCHUP_BARS`; pair
  it with JetStream, which drops the replayed duplicates by `event_id`.
- **Errors in their own Telegram chat.** `TELEGRAM_LOG_ERRORS_ENABLED` mirrors
  every `ERROR` record to `TELEGRAM_LOG_CHAT_IDS` with an optional separate
  bot token, its own queue and a `TELEGRAM_LOG_DEDUP_WINDOW` suppression
  window.

### Changed

- **Start-up failures no longer stop the process.** An unreachable NATS or a
  gateway that will not start is logged, reported as **Ingester Degraded** and
  survived; NATS reconnects underneath. A supervisor restart would only drop
  more bars.
- One malformed record on one symbol is logged and skipped instead of aborting
  the whole poll cycle, so it can no longer starve the other symbols.
- JetStream: an existing stream whose subjects miss `NATS_SUBJECT_PREFIX`, or
  whose duplicate window is shorter than configured, is now reported. Streams
  are still never reconfigured.
- The **Ingester Failed To Start** notification is gone: there is no longer a
  start-up path that reports it. **Ingester Degraded** replaces it.
- Spelling harmonised repository-wide: `ingestor` → `ingester`. The Python
  package is now `ingester/` (`python -m ingester`), the project is
  `algo-trading-ingester`, and `APP_NAME` defaults to `algo-trading-ingester`.
  No environment variable name changed.
