###############################################################
#
# BASIC BEHAVIORS
#
###############################################################

load("src/parser.jl")

delimiter = ','
quote_character = '"'

column_names = Array(ASCIIString, 0)
types = {ASCIIString, ASCIIString, ASCIIString, Float64, Int64}

md = DataFrameMetaData(column_names, types)

line1 = "a,\"b\",\"c,d\",1.0,1"
line2 = "a,\"b\",\"\",,"

df1 = parse_delimited_line(line1, delimiter, quote_character, md)
df2 = parse_delimited_line(line2, delimiter, quote_character, md)

true_df = DataFrame(quote
  x1 = DataVec["a", "a"]
  x2 = DataVec["b", "b"]
  x3 = DataVec["c,d", NA]
  x4 = DataVec[1.0, NA]
  x5 = DataVec[1, NA]
end)

@assert df1 == true_df[1, :]
@assert df2 == true_df[2, :]
@assert rbind(df1, df2) == true_df

df_final = parse_delimited_file("test/data/complex_data.csv", delimiter, quote_character, md)

@assert df_final == true_df

md.column_names = ["A", "B", "C", "D", "E"]
df_final = parse_delimited_file("test/data/complex_data.csv", delimiter, quote_character, md)

# This first equality test succeeds. Why?
#@assert df_final == true_df
@assert names(df_final) != names(true_df)

df = DataFrame(10, 5)

column_names = Array(ASCIIString, 0)
types = {ASCIIString, ASCIIString, ASCIIString, Float64, Int64}

md = DataFrameMetaData(column_names, types)

df = DataFrame(10, md)

###############################################################
#
# TIMING TESTS
#
###############################################################

@elapsed for i = 1:1_000
  df_final = parse_delimited_file("test/data/simple_data.csv", delimiter, quote_character, md)
end

# Run to force JIT'ing
df_final = alt_parse_delimited_file("test/data/simple_data.csv", delimiter, quote_character, md)

@elapsed for i = 1:1_000
  df_final = alt_parse_delimited_file("test/data/simple_data.csv", delimiter, quote_character, md)
end

# Run to force JIT'ing
df_final = csvDataFrame("test/data/simple_data.csv")

@elapsed for i = 1:1_000
  df_final = csvDataFrame("test/data/simple_data.csv")
end

# Run to force JIT'ing
df_final = alt2_parse_delimited_file("test/data/simple_data.csv", 2, delimiter, quote_character, md)

@elapsed for i = 1:1_000
  df_final = alt2_parse_delimited_file("test/data/simple_data.csv", 2, delimiter, quote_character, md)
end

# Does this depend on the number of columns?
# Also need to measure increase in memory footprint
