defmodule Duxedo.Store do
  @moduledoc false

  use GenServer
  require Logger

  @create_observations """
  CREATE TABLE IF NOT EXISTS observations (
    ts BIGINT NOT NULL,
    event VARCHAR NOT NULL,
    field VARCHAR NOT NULL,
    value DOUBLE NOT NULL,
    tags VARCHAR DEFAULT '{}',
    session VARCHAR
  )
  """

  @create_events """
  CREATE TABLE IF NOT EXISTS events (
    ts BIGINT NOT NULL,
    name VARCHAR NOT NULL,
    measurements VARCHAR NOT NULL,
    tags VARCHAR DEFAULT '{}',
    session VARCHAR
  )
  """

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: name(args[:instance]))
  end

  def memory_conn(instance \\ :duxedo) do
    :persistent_term.get({__MODULE__, instance, :memory})
  end

  def disk_conn(instance \\ :duxedo) do
    :persistent_term.get({__MODULE__, instance, :disk})
  end

  @doc """
  Move rows older than the in-memory retention window from the memory
  DB to the disk DB.

  ## Durability

  Adbc/DuckDB writes are buffered. Rows are durable on disk after the
  next DuckDB checkpoint or on graceful shutdown via `terminate/2`,
  not the moment this call returns. If you need write-then-crash
  durability, call this and then `Adbc.Connection.query!(disk_conn,
  "CHECKPOINT")` explicitly.

  ## Failure mode

  If `transfer_table/4` raises after inserting some rows on disk but
  before the matching memory `DELETE` runs, the next flush will move
  the same rows again. `observations` has no unique key, so duplicates
  accumulate. We accept this rather than wrapping the cross-DB move in
  a transaction — operators get at-least-once flush semantics.
  """
  def flush_to_disk(instance \\ :duxedo) do
    GenServer.call(name(instance), :flush_to_disk)
  end

  @doc """
  Apply retention by deleting rows older than the configured windows
  on both the memory and disk DBs.
  """
  def run_retention(instance \\ :duxedo) do
    GenServer.call(name(instance), :run_retention)
  end

  defp name(instance), do: Module.concat(__MODULE__, instance)

  # init/1 opens the DuckDB connections synchronously rather than
  # offloading to handle_continue/2. The :rest_for_one supervisor
  # starts Collector after Store, and Collector reads the conns from
  # :persistent_term, so Store's init must complete before the next
  # child starts. Moving the work to handle_continue would introduce
  # a race; the work is bounded (DuckDB open + 4 CREATE TABLE IF NOT
  # EXISTS) and fast enough in practice not to need deferral.
  @impl GenServer
  def init(args) do
    Process.flag(:trap_exit, true)

    instance = args[:instance]
    persistence_dir = args[:persistence_dir] || "/data/duxedo"
    memory_limit = args[:memory_limit] || "64MB"
    flush_interval = (args[:flush_interval] || 300) * 1_000
    retention_interval = (args[:retention_interval] || 600) * 1_000
    retention = args[:retention] || [memory: {1, :hour}, disk: {30, :day}]

    persistence_path = Path.join(persistence_dir, to_string(instance))
    File.mkdir_p!(persistence_path)

    # Start in-memory DuckDB
    {:ok, mem_db} = Adbc.Database.start_link(driver: :duckdb)
    {:ok, mem_conn} = Adbc.Connection.start_link(database: mem_db)
    Adbc.Connection.query!(mem_conn, "SET memory_limit = '#{memory_limit}'")

    # Start on-disk DuckDB
    disk_path = Path.join(persistence_path, "archive.duckdb")
    {:ok, disk_db} = Adbc.Database.start_link(driver: :duckdb, path: disk_path)
    {:ok, disk_conn} = Adbc.Connection.start_link(database: disk_db)

    # Create tables on both
    for conn <- [mem_conn, disk_conn] do
      Adbc.Connection.query!(conn, @create_observations)
      Adbc.Connection.query!(conn, @create_events)
    end

    # Publish connections for zero-cost access
    :persistent_term.put({__MODULE__, instance, :memory}, mem_conn)
    :persistent_term.put({__MODULE__, instance, :disk}, disk_conn)

    # Register for clock sync
    if args[:clock] do
      Duxedo.TimeServer.register(instance, self())
    end

    state = %{
      instance: instance,
      memory_conn: mem_conn,
      disk_conn: disk_conn,
      persistence_path: persistence_path,
      flush_interval: flush_interval,
      retention_interval: retention_interval,
      retention: retention
    }

    schedule_flush(flush_interval)
    schedule_retention(retention_interval)

    {:ok, state}
  end

  @impl GenServer
  def handle_call(:flush_to_disk, _from, state) do
    do_flush(state)
    {:reply, :ok, state}
  end

  def handle_call(:run_retention, _from, state) do
    do_retention(state)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info(:flush_to_disk, state) do
    do_flush(state)
    schedule_flush(state.flush_interval)
    {:noreply, state}
  end

  def handle_info(:run_retention, state) do
    do_retention(state)
    schedule_retention(state.retention_interval)
    {:noreply, state}
  end

  # Only the in-memory DB is adjusted: disk rows were written in a
  # previous run when the clock was assumed correct, so their ts
  # values are real and must not move.
  def handle_info({Duxedo.TimeServer, adjustment}, state) do
    if adjustment != 0 do
      Logger.info("Duxedo: adjusting timestamps by #{adjustment}s after clock sync")

      Adbc.Connection.query!(
        state.memory_conn,
        "UPDATE observations SET ts = ts + #{adjustment}"
      )

      Adbc.Connection.query!(
        state.memory_conn,
        "UPDATE events SET ts = ts + #{adjustment}"
      )
    end

    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    do_flush(state)
  rescue
    e in Adbc.Error ->
      Logger.error("Duxedo: failed to flush on shutdown: #{Exception.message(e)}")
  end

  # Note: a partial failure here can double-count rows on the next
  # flush — transfer_table may insert some rows on disk before raising,
  # and the memory DELETE never runs. Observations has no unique key so
  # duplicates accumulate. Acceptable for the happy path; document.
  defp do_flush(state) do
    cutoff = System.system_time(:second) - retention_seconds(state.retention[:memory])

    # Transfer observations from memory to disk via Arrow columns
    transfer_table(state.memory_conn, state.disk_conn, "observations", cutoff)
    transfer_table(state.memory_conn, state.disk_conn, "events", cutoff)

    # Delete the transferred data from memory
    Adbc.Connection.query!(state.memory_conn, "DELETE FROM observations WHERE ts < #{cutoff}")
    Adbc.Connection.query!(state.memory_conn, "DELETE FROM events WHERE ts < #{cutoff}")
  rescue
    e in Adbc.Error ->
      Logger.warning("Duxedo: flush to disk failed: #{Exception.message(e)}")
  end

  defp transfer_table(src_conn, dst_conn, table, cutoff) do
    result = Adbc.Connection.query!(src_conn, "SELECT * FROM #{table} WHERE ts < #{cutoff}")

    case result.data do
      [batch] when batch != [] ->
        columns = Enum.map(batch, &Adbc.Column.materialize/1)
        Adbc.Connection.bulk_insert!(dst_conn, columns, table: table, mode: :append)

      _ ->
        :ok
    end
  end

  defp do_retention(state) do
    now = System.system_time(:second)

    mem_cutoff = now - retention_seconds(state.retention[:memory])
    disk_cutoff = now - retention_seconds(state.retention[:disk])

    Adbc.Connection.query!(state.memory_conn, "DELETE FROM observations WHERE ts < #{mem_cutoff}")
    Adbc.Connection.query!(state.memory_conn, "DELETE FROM events WHERE ts < #{mem_cutoff}")

    Adbc.Connection.query!(state.disk_conn, "DELETE FROM observations WHERE ts < #{disk_cutoff}")
    Adbc.Connection.query!(state.disk_conn, "DELETE FROM events WHERE ts < #{disk_cutoff}")
  rescue
    e in Adbc.Error ->
      Logger.warning("Duxedo: retention failed: #{Exception.message(e)}")
  end

  defp retention_seconds({n, :second}), do: n
  defp retention_seconds({n, :minute}), do: n * 60
  defp retention_seconds({n, :hour}), do: n * 3600
  defp retention_seconds({n, :day}), do: n * 86_400
  defp retention_seconds(nil), do: 3600

  defp schedule_flush(interval), do: Process.send_after(self(), :flush_to_disk, interval)
  defp schedule_retention(interval), do: Process.send_after(self(), :run_retention, interval)
end
