load("src/convert.jl")

items = ["a", "b", "c,d", "1.0", "1"]
column_names = Array(String, 0)
types = {String, String, String, Float64, Int64}

md = DataFrameMetaData(column_names, types)

(results, is_missing) = convert(md, items)

@assert all(results .== {"a", "b", "c,d", 1.0, 1})

items = ["a", "b", "", "", ""]

(results, is_missing) = convert(md, items)

@assert all(results .== {"a", "b", "", 0.0, 0})
@assert all(is_missing .== [false, false, true, true, true])
