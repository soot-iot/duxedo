# Changelog

All notable changes to `duxedo` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project adheres to semantic versioning.

## [Unreleased]

### Added
- Configurable `:retention_interval` option (was hardcoded to 600 s).
- `Duxedo` defdelegate surface now mirrors `Duxedo.Query` for
  `observations`, `events`, `sum`, `distribution`, `bucket`.

### Changed
- Collector now traps exits so `terminate/2` runs on supervisor
  shutdown and `:telemetry` handlers no longer leak across restarts.
- Store / Collector rescue blocks narrowed from `_` to `Adbc.Error`
  (DB paths) and a fixed list of user-callback errors (telemetry
  handlers). Bugs propagate; transient DB errors keep their previous
  log-and-continue behaviour.
- Session ids are generated via `:crypto.strong_rand_bytes/1` instead
  of `UUID.uuid4/0`, dropping the `:elixir_uuid` dependency.
- `:adbc` dep tightened from `~> 0.7` to `~> 0.11`.

### Removed
- Empty `Duxedo.Application` module and the `mod:` line in `mix.exs`.
  The library is added as a child of the user's supervisor; nothing
  needs to autostart.
- Unused `:term_ui` optional dep.

### Fixed
- `Duxedo.Export.to_arrow_ipc/1` interpolated a `%Dux.TableRef{}`
  struct directly into SQL — works only against an older Dux that
  used a string. Pattern-match `.name` explicitly.

## [0.1.0]

### Added
- Initial release: telemetry collector, in-memory + on-disk DuckDB
  store via ADBC, query helpers (`last_value`, `summary`, `count`,
  `sum`, `percentiles`, `series`, `distribution`, `bucket`,
  `observations`, `events`), Arrow IPC and CSV export, ASCII chart
  plotting, and `Duxedo.Clock` behaviour for Nerves clock-sync
  timestamp adjustment.
