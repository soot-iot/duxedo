# Adbc.Result.data is typed as [Adbc.Column.t()] upstream but at
# runtime the value is a list of batches — [[Adbc.Column.t()]] — so
# pattern matches like `[[col]]` are correct. Filter the resulting
# pattern_match warnings until upstream tightens the spec.
[
  {"lib/duxedo/query.ex", :pattern_match},
  {"lib/duxedo/query.ex", :pattern_match}
]
