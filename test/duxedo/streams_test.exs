defmodule Duxedo.StreamsTest do
  use ExUnit.Case, async: false

  alias Duxedo.Streams

  setup ctx do
    instance =
      :"streams_t_#{ctx.test |> Atom.to_string() |> String.replace(~r/[^a-zA-Z0-9]/, "_") |> String.slice(0, 50)}"

    tmp_dir =
      System.tmp_dir!() |> Path.join("duxedo_streams_#{:erlang.unique_integer([:positive])}")

    File.mkdir_p!(tmp_dir)

    start_supervised!(
      {Duxedo,
       [
         instance: instance,
         persistence_dir: tmp_dir,
         memory_limit: "32MB",
         flush_interval: 3600,
         collect_interval: 3600,
         metrics: [],
         events: []
       ]}
    )

    on_exit(fn -> File.rm_rf!(tmp_dir) end)

    %{instance: instance}
  end

  test "define/3 creates a typed table; redefining the same schema is a no-op", %{
    instance: instance
  } do
    cols = [{:seq, :s64}, {:ts, :s64}, {:x, :f64}]
    assert :ok = Streams.define(:vibration, cols, instance: instance)
    assert :ok = Streams.define(:vibration, cols, instance: instance)
  end

  test "redefining with a different schema is rejected", %{instance: instance} do
    Streams.define(:vibration, [{:seq, :s64}, {:x, :f64}], instance: instance)

    assert {:error, :schema_conflict} =
             Streams.define(:vibration, [{:seq, :s64}, {:x, :s64}], instance: instance)
  end

  test "bulk_append/2 + take_oldest/2 round-trips rows oldest-first", %{instance: instance} do
    Streams.define(:vibration, [{:seq, :s64}, {:ts, :s64}, {:x, :f64}], instance: instance)

    rows = [
      %{seq: 1, ts: 1000, x: 0.1},
      %{seq: 2, ts: 1010, x: 0.2},
      %{seq: 3, ts: 1020, x: 0.3}
    ]

    :ok = Streams.bulk_append(:vibration, rows, instance: instance)

    {:ok, taken} = Streams.take_oldest(:vibration, 2, instance: instance)
    assert Enum.map(taken, & &1.seq) == [1, 2]
    assert Enum.map(taken, & &1.x) == [0.1, 0.2]
  end

  test "drop_through/2 deletes rows up to and including the seq", %{instance: instance} do
    Streams.define(:metrics, [{:seq, :s64}, {:value, :f64}], instance: instance)

    Streams.bulk_append(
      :metrics,
      Enum.map(1..5, fn n -> %{seq: n, value: n * 1.0} end),
      instance: instance
    )

    :ok = Streams.drop_through(:metrics, 3, instance: instance)

    {:ok, taken} = Streams.take_oldest(:metrics, 100, instance: instance)
    assert Enum.map(taken, & &1.seq) == [4, 5]
  end

  test "stats/1 reports row count and seq range", %{instance: instance} do
    Streams.define(:s, [{:seq, :s64}, {:v, :s64}], instance: instance)

    assert {:ok, %{rows: 0, min_seq: nil, max_seq: nil}} =
             Streams.stats(:s, instance: instance)

    Streams.bulk_append(
      :s,
      Enum.map([10, 11, 12], fn n -> %{seq: n, v: n} end),
      instance: instance
    )

    assert {:ok, %{rows: 3, min_seq: 10, max_seq: 12}} =
             Streams.stats(:s, instance: instance)
  end

  test "to_arrow_ipc/2 returns Arrow IPC bytes + seq range; from_ipc_stream round-trips", %{
    instance: instance
  } do
    Streams.define(:vibration, [{:seq, :s64}, {:x, :f64}, {:y, :f64}], instance: instance)

    Streams.bulk_append(
      :vibration,
      [
        %{seq: 1, x: 0.1, y: 1.1},
        %{seq: 2, x: 0.2, y: 2.2}
      ],
      instance: instance
    )

    {:ok, %{ipc: ipc, min_seq: 1, max_seq: 2, rows: 2}} =
      Streams.to_arrow_ipc(:vibration, instance: instance, max_rows: 10)

    assert is_binary(ipc)
    assert byte_size(ipc) > 0

    # Round-trip: feed the IPC bytes back into Adbc and read the columns out.
    {:ok, %Adbc.Result{data: [batch]}} = Adbc.Result.from_ipc_stream(ipc)
    cols = Enum.map(batch, &Adbc.Column.materialize/1)
    seq_col = Enum.find(cols, &(&1.field.name == "seq"))
    assert Adbc.Column.to_list(seq_col) == [1, 2]
  end

  test "to_arrow_ipc/2 returns :empty when nothing is buffered", %{instance: instance} do
    Streams.define(:empty, [{:seq, :s64}], instance: instance)
    assert {:error, :empty} = Streams.to_arrow_ipc(:empty, instance: instance)
  end

  test "max_rows retention prunes oldest entries on append", %{instance: instance} do
    Streams.define(:cap, [{:seq, :s64}, {:v, :s64}],
      instance: instance,
      max_rows: 3
    )

    Streams.bulk_append(
      :cap,
      Enum.map(1..10, fn n -> %{seq: n, v: n} end),
      instance: instance
    )

    {:ok, %{rows: rows}} = Streams.stats(:cap, instance: instance)
    assert rows <= 3

    {:ok, taken} = Streams.take_oldest(:cap, 100, instance: instance)
    # Pruning drops the oldest; the surviving rows are the most recent ones.
    assert List.last(taken).seq == 10
  end

  test "raises on invalid identifier characters", %{instance: instance} do
    assert_raise ArgumentError, fn ->
      Streams.define(:"weird name", [{:seq, :s64}], instance: instance)
    end
  end

  test "raises on unknown column type" do
    assert_raise ArgumentError, ~r/unsupported/, fn ->
      Streams.define(:bad, [{:seq, :s64}, {:x, :unicorn}])
    end
  end

  test "operations on an undefined stream return :error", %{instance: instance} do
    assert :error = Streams.take_oldest(:nope, 1, instance: instance)
    assert :error = Streams.drop_through(:nope, 0, instance: instance)
    assert :error = Streams.stats(:nope, instance: instance)
  end
end
