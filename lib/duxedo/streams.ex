defmodule Duxedo.Streams do
  @moduledoc """
  Typed tabular streams on top of Duxedo's DuckDB store.

  Each stream is one DuckDB table. Callers `define/3` the schema,
  `bulk_append/2` rows, `take_oldest/2` the next batch, then either
  `drop_through/2` after a successful upload or `to_arrow_ipc/2` to
  build an Arrow IPC payload for the wire.

  This module is intentionally soot-agnostic: any Nerves device that
  wants a "buffer some tabular data, ship it later" workflow uses
  this directly. The soot framework consumes it via
  `SootDeviceProtocol.Telemetry.Buffer.Duxedo`.

  ## Schemas

  Schemas are an ordered list of `{column_name, type, opts}` tuples
  (the `opts` element is itself optional):

      Duxedo.Streams.define(:vibration, [
        {:seq, :s64},
        {:ts, :s64},
        {:x, :f64},
        {:y, :f64},
        {:z, :f64}
      ])

  Supported types map to DuckDB column types:

  | Duxedo  | DuckDB    |
  |---------|-----------|
  | :s8     | TINYINT   |
  | :s16    | SMALLINT  |
  | :s32    | INTEGER   |
  | :s64    | BIGINT    |
  | :u8     | UTINYINT  |
  | :u16    | USMALLINT |
  | :u32    | UINTEGER  |
  | :u64    | UBIGINT   |
  | :f32    | REAL      |
  | :f64    | DOUBLE    |
  | :bool   | BOOLEAN   |
  | :string | VARCHAR   |
  | :bytes  | BLOB      |

  ## Sequences

  By convention the first column is the monotonic sequence used for
  ordering and pruning. Override with `seq_col: :name` when defining.

  ## Retention

  Each stream defaults to a row budget; when the budget is exceeded
  on `bulk_append/2` the oldest rows are pruned. Configure with
  `:max_rows` and `:max_bytes` (rough — DuckDB's cardinality counts
  not on-disk size). Bytes can be queried via `stats/1`.
  """

  alias Duxedo.Store

  @type type ::
          :s8
          | :s16
          | :s32
          | :s64
          | :u8
          | :u16
          | :u32
          | :u64
          | :f32
          | :f64
          | :bool
          | :string
          | :bytes

  @type column :: {atom(), type()} | {atom(), type(), keyword()}

  @duckdb_types %{
    s8: "TINYINT",
    s16: "SMALLINT",
    s32: "INTEGER",
    s64: "BIGINT",
    u8: "UTINYINT",
    u16: "USMALLINT",
    u32: "UINTEGER",
    u64: "UBIGINT",
    f32: "REAL",
    f64: "DOUBLE",
    bool: "BOOLEAN",
    string: "VARCHAR",
    bytes: "BLOB"
  }

  # ─── definition ─────────────────────────────────────────────────────

  @doc """
  Declare a stream. Creates the underlying DuckDB table if it does
  not already exist; idempotent on the schema. Re-defining a stream
  with a different schema is rejected with `{:error,
  :schema_conflict}`.

  Options:

    * `:instance`  — Duxedo instance, default `:duxedo`.
    * `:seq_col`   — name of the monotonic sequence column. Default
                     `:seq` (or the first column if `:seq` isn't
                     present).
    * `:max_rows`  — retention budget in rows. Default `1_000_000`.
    * `:max_bytes` — retention budget in approximate bytes (counted
                     on append using `:erlang.term_to_binary/1`).
                     Default `64 * 1024 * 1024` (64 MiB).
  """
  @spec define(atom(), [column()], keyword()) :: :ok | {:error, term()}
  def define(name, columns, opts \\ []) when is_atom(name) and is_list(columns) do
    instance = Keyword.get(opts, :instance, :duxedo)
    columns = Enum.map(columns, &normalize_column/1)
    seq_col = Keyword.get(opts, :seq_col, default_seq_col(columns))

    spec = %{
      name: name,
      columns: columns,
      seq_col: seq_col,
      max_rows: Keyword.get(opts, :max_rows, 1_000_000),
      max_bytes: Keyword.get(opts, :max_bytes, 64 * 1024 * 1024)
    }

    case lookup(instance, name) do
      {:ok, ^spec} ->
        :ok

      {:ok, _existing} ->
        {:error, :schema_conflict}

      :error ->
        do_define(instance, spec)
    end
  end

  defp do_define(instance, spec) do
    conn = Store.memory_conn(instance)
    sql = build_create_sql(spec)
    Adbc.Connection.query!(conn, sql)
    register(instance, spec)
    :ok
  end

  defp build_create_sql(%{name: name, columns: columns}) do
    cols_sql =
      columns
      |> Enum.map(fn {col_name, type, _opts} ->
        "#{quote_ident(col_name)} #{Map.fetch!(@duckdb_types, type)}"
      end)
      |> Enum.join(", ")

    "CREATE TABLE IF NOT EXISTS #{quote_ident(name)} (#{cols_sql})"
  end

  defp normalize_column({name, type}) when is_atom(name) and is_atom(type),
    do: normalize_column({name, type, []})

  defp normalize_column({name, type, opts})
       when is_atom(name) and is_atom(type) and is_list(opts) do
    unless Map.has_key?(@duckdb_types, type) do
      raise ArgumentError, "unsupported Duxedo.Streams type #{inspect(type)}"
    end

    {name, type, opts}
  end

  defp default_seq_col(columns) do
    case Enum.find(columns, fn {n, _, _} -> n == :seq end) do
      nil ->
        {first, _type, _opts} = hd(columns)
        first

      {name, _, _} ->
        name
    end
  end

  # ─── writes ─────────────────────────────────────────────────────────

  @doc "Append a single row."
  @spec append(atom(), map(), keyword()) :: :ok | {:error, term()}
  def append(name, row, opts \\ []), do: bulk_append(name, [row], opts)

  @doc "Append multiple rows in one DuckDB bulk insert."
  @spec bulk_append(atom(), [map()], keyword()) :: :ok | {:error, term()}
  def bulk_append(_name, [], _opts), do: :ok

  def bulk_append(name, rows, opts) when is_atom(name) and is_list(rows) do
    instance = Keyword.get(opts, :instance, :duxedo)

    with {:ok, spec} <- lookup(instance, name) do
      conn = Store.memory_conn(instance)
      columns = build_adbc_columns(spec.columns, rows)
      Adbc.Connection.bulk_insert!(conn, columns, table: to_string(name), mode: :append)
      _ = maybe_prune(instance, spec)
      :ok
    end
  end

  defp build_adbc_columns(col_specs, rows) do
    Enum.map(col_specs, fn {col_name, type, _opts} ->
      values = Enum.map(rows, &Map.fetch!(&1, col_name))
      column_for(type, values, col_name)
    end)
  end

  defp column_for(type, values, name) do
    fun = column_fn(type)
    fun.(values, name: to_string(name))
  end

  defp column_fn(:s8), do: &Adbc.Column.s8/2
  defp column_fn(:s16), do: &Adbc.Column.s16/2
  defp column_fn(:s32), do: &Adbc.Column.s32/2
  defp column_fn(:s64), do: &Adbc.Column.s64/2
  defp column_fn(:u8), do: &Adbc.Column.u8/2
  defp column_fn(:u16), do: &Adbc.Column.u16/2
  defp column_fn(:u32), do: &Adbc.Column.u32/2
  defp column_fn(:u64), do: &Adbc.Column.u64/2
  defp column_fn(:f32), do: &Adbc.Column.f32/2
  defp column_fn(:f64), do: &Adbc.Column.f64/2
  defp column_fn(:bool), do: &Adbc.Column.boolean/2
  defp column_fn(:string), do: &Adbc.Column.string/2
  defp column_fn(:bytes), do: &Adbc.Column.binary/2

  # ─── reads ──────────────────────────────────────────────────────────

  @doc """
  Pull the next `max_rows` oldest entries as a list of row maps.
  Used by callers that want to inspect rows before uploading.
  """
  @spec take_oldest(atom(), pos_integer(), keyword()) ::
          {:ok, [map()]} | {:error, term()}
  def take_oldest(name, max_rows, opts \\ []) do
    instance = Keyword.get(opts, :instance, :duxedo)

    with {:ok, spec} <- lookup(instance, name) do
      conn = Store.memory_conn(instance)
      seq = quote_ident(spec.seq_col)
      table = quote_ident(spec.name)
      sql = "SELECT * FROM #{table} ORDER BY #{seq} ASC LIMIT #{max_rows}"

      result = Adbc.Connection.query!(conn, sql)
      {:ok, result_to_rows(result, spec)}
    end
  end

  @doc """
  Build an Arrow IPC stream of the next `max_rows` oldest rows.
  Returns `{:ok, %{ipc: bytes, min_seq: integer, max_seq: integer,
  rows: integer}}` or `{:error, :empty}` if the stream has nothing
  buffered.
  """
  @spec to_arrow_ipc(atom(), keyword()) :: {:ok, map()} | {:error, term()}
  def to_arrow_ipc(name, opts \\ []) do
    instance = Keyword.get(opts, :instance, :duxedo)
    max_rows = Keyword.get(opts, :max_rows, 1_000)

    case lookup(instance, name) do
      :error ->
        :error

      {:ok, spec} ->
        case preview(spec, instance, max_rows) do
          {:ok, %{rows: 0}} ->
            {:error, :empty}

          {:ok, %{min_seq: min_seq, max_seq: max_seq, rows: rows}} ->
            ipc = dump_ipc(spec, instance, max_rows)
            {:ok, %{ipc: ipc, min_seq: min_seq, max_seq: max_seq, rows: rows}}
        end
    end
  end

  defp preview(spec, instance, max_rows) do
    conn = Store.memory_conn(instance)
    seq = quote_ident(spec.seq_col)
    table = quote_ident(spec.name)

    sql = """
    SELECT MIN(#{seq}) AS min_seq, MAX(#{seq}) AS max_seq, COUNT(*) AS row_count
    FROM (SELECT #{seq} FROM #{table} ORDER BY #{seq} ASC LIMIT #{max_rows})
    """

    result = Adbc.Connection.query!(conn, sql)

    case result.data do
      [batch] ->
        cols = Enum.map(batch, &Adbc.Column.materialize/1)

        {min_seq, max_seq, row_count} =
          {value(cols, "min_seq"), value(cols, "max_seq"), value(cols, "row_count") || 0}

        {:ok, %{min_seq: min_seq, max_seq: max_seq, rows: row_count}}

      _ ->
        {:ok, %{min_seq: nil, max_seq: nil, rows: 0}}
    end
  end

  defp dump_ipc(spec, instance, max_rows) do
    conn = Store.memory_conn(instance)
    seq = quote_ident(spec.seq_col)
    table = quote_ident(spec.name)

    sql = "SELECT * FROM #{table} ORDER BY #{seq} ASC LIMIT #{max_rows}"

    {:ok, ipc} =
      Adbc.Connection.query_pointer(conn, sql, fn stream_result ->
        Adbc.StreamResult.to_ipc_stream(stream_result)
      end)

    ipc
  end

  # ─── deletes ────────────────────────────────────────────────────────

  @doc "Drop every row whose sequence is `<= up_to_seq`."
  @spec drop_through(atom(), integer(), keyword()) :: :ok | {:error, term()}
  def drop_through(name, up_to_seq, opts \\ []) when is_integer(up_to_seq) do
    instance = Keyword.get(opts, :instance, :duxedo)

    with {:ok, spec} <- lookup(instance, name) do
      conn = Store.memory_conn(instance)
      seq = quote_ident(spec.seq_col)
      table = quote_ident(spec.name)
      Adbc.Connection.query!(conn, "DELETE FROM #{table} WHERE #{seq} <= #{up_to_seq}")
      :ok
    end
  end

  @doc "Drop every row in the stream."
  @spec truncate(atom(), keyword()) :: :ok | {:error, term()}
  def truncate(name, opts \\ []) do
    instance = Keyword.get(opts, :instance, :duxedo)

    with {:ok, spec} <- lookup(instance, name) do
      conn = Store.memory_conn(instance)
      Adbc.Connection.query!(conn, "DELETE FROM #{quote_ident(spec.name)}")
      :ok
    end
  end

  # ─── stats ──────────────────────────────────────────────────────────

  @doc """
  Row count and (approximate) byte count for the stream. Bytes are
  counted on `bulk_append/2` and reset on `drop_through/2`; they are
  rough estimates intended for retention triggers, not accurate
  on-disk sizes.
  """
  @spec stats(atom(), keyword()) :: {:ok, map()} | {:error, term()}
  def stats(name, opts \\ []) do
    instance = Keyword.get(opts, :instance, :duxedo)

    with {:ok, spec} <- lookup(instance, name) do
      conn = Store.memory_conn(instance)
      seq = quote_ident(spec.seq_col)
      table = quote_ident(spec.name)

      sql = """
      SELECT COUNT(*) AS row_count, MIN(#{seq}) AS min_seq, MAX(#{seq}) AS max_seq
      FROM #{table}
      """

      result = Adbc.Connection.query!(conn, sql)

      case result.data do
        [batch] ->
          cols = Enum.map(batch, &Adbc.Column.materialize/1)

          {:ok,
           %{
             rows: value(cols, "row_count") || 0,
             min_seq: value(cols, "min_seq"),
             max_seq: value(cols, "max_seq")
           }}

        _ ->
          {:ok, %{rows: 0, min_seq: nil, max_seq: nil}}
      end
    end
  end

  # ─── pruning ────────────────────────────────────────────────────────

  defp maybe_prune(instance, %{max_rows: max_rows} = spec) when is_integer(max_rows) do
    conn = Store.memory_conn(instance)
    seq = quote_ident(spec.seq_col)
    table = quote_ident(spec.name)

    sql = "SELECT COUNT(*) AS c FROM #{table}"
    result = Adbc.Connection.query!(conn, sql)

    case result.data do
      [batch] ->
        cols = Enum.map(batch, &Adbc.Column.materialize/1)
        rows = value(cols, "c") || 0

        if rows > max_rows do
          excess = rows - max_rows

          Adbc.Connection.query!(conn, """
          DELETE FROM #{table} WHERE #{seq} IN (
            SELECT #{seq} FROM #{table} ORDER BY #{seq} ASC LIMIT #{excess}
          )
          """)
        end

      _ ->
        :ok
    end

    :ok
  end

  defp maybe_prune(_instance, _spec), do: :ok

  # ─── registry ───────────────────────────────────────────────────────

  defp register(instance, spec) do
    :persistent_term.put({__MODULE__, instance, spec.name}, spec)
  end

  defp lookup(instance, name) do
    case :persistent_term.get({__MODULE__, instance, name}, :missing) do
      :missing -> :error
      spec -> {:ok, spec}
    end
  end

  # ─── helpers ────────────────────────────────────────────────────────

  defp quote_ident(name) when is_atom(name), do: quote_ident(Atom.to_string(name))

  defp quote_ident(name) when is_binary(name) do
    unless Regex.match?(~r/\A[A-Za-z_][A-Za-z0-9_]*\z/, name) do
      raise ArgumentError, "invalid Duxedo.Streams identifier #{inspect(name)}"
    end

    "\"" <> name <> "\""
  end

  defp result_to_rows(%Adbc.Result{data: data}, spec) do
    Enum.flat_map(data, fn batch ->
      named =
        batch
        |> Enum.map(fn col ->
          materialized = Adbc.Column.materialize(col)
          {materialized.field.name, Adbc.Column.to_list(materialized)}
        end)
        |> Map.new()

      transpose(named, spec.columns)
    end)
  end

  defp transpose(named, _col_specs) when map_size(named) == 0, do: []

  defp transpose(named, col_specs) do
    len = named |> Map.values() |> hd() |> length()

    Enum.map(0..(len - 1), fn i ->
      Map.new(col_specs, fn {col_name, _type, _opts} ->
        list = Map.fetch!(named, to_string(col_name))
        {col_name, Enum.at(list, i)}
      end)
    end)
  end

  defp value(cols, target_name) do
    case Enum.find(cols, fn col -> col.field.name == target_name end) do
      nil ->
        nil

      col ->
        case col |> Adbc.Column.materialize() |> Adbc.Column.to_list() do
          [v | _] -> v
          _ -> nil
        end
    end
  end
end
