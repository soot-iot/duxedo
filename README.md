# Duxedo

Device metrics on DuckDB and Arrow.

Duxedo collects BEAM `:telemetry` metrics into an in-memory DuckDB
database, periodically flushes older rows to a co-located on-disk
DuckDB, and exposes the results as composable
[Dux](https://hex.pm/packages/dux) dataframes or Arrow IPC for upload.

## Installation

```elixir
def deps do
  [
    {:duxedo, "~> 0.1.0"}
  ]
end
```

Add it to your supervision tree with the metrics and events you want
captured:

```elixir
alias Telemetry.Metrics

children = [
  {Duxedo,
   metrics: [
     Metrics.last_value("vm.memory.total"),
     Metrics.counter("http.request.count"),
     Metrics.summary("http.request.duration", tags: [:method])
   ],
   events: ["nerves.dhcp.lease"],
   persistence_dir: "/data/duxedo",
   memory_limit: "64MB"}
]
```

## Querying

```elixir
Duxedo.last_value("vm.memory.total")
Duxedo.summary("http.request.duration", last: {5, :minute})
Duxedo.percentiles("http.request.duration", [50, 90, 99])
Duxedo.series("vm.memory.total", last: {1, :hour})
Duxedo.plot("vm.memory.total")
```

The `Duxedo.Query` and `Duxedo.Export` modules expose the full
surface, including `observations/2`, `events/2`, `bucket/3`,
`distribution/2`, `to_csv/2`, and `to_arrow_ipc/2`.

## Clock sync on Nerves

On Nerves devices the system clock may not be set at boot. Pass a
module implementing `Duxedo.Clock`:

```elixir
{Duxedo, clock: NervesTime, ...}
```

In-memory timestamps are adjusted forward once `clock.synchronized?/0`
returns `true`.

## Documentation

Full docs at <https://hexdocs.pm/duxedo>.
