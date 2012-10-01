load("src/split.jl")
load("src/convert.jl")

load("src/init.jl")

function DataFrame(n::Int64, p::Int64)
  columns = Array(Any, p)
  names = Array(ByteString, p)
  for j in 1:p
    names[j] = "x$(j)"
    columns[j] = DataVec(Array(Int64, n), Array(Bool, n))
    for i in 1:n
      columns[j][i] = NA
    end
  end
  DataFrame(columns, Index(names))
end

function DataFrame(n::Int64, md::DataFrameMetaData)
  p = length(md.types)
  columns = Array(Any, p)
  if md.column_names == []
    names = Array(ByteString, p)
  else
    names = md.column_names
  end
  for j in 1:p
    names[j] = "x$(j)"
    columns[j] = DataVec(Array(md.types[j], n), Array(Bool, n))
    for i in 1:n
      columns[j][i] = missing_value_defaults[md.types[j]]
      columns[j][i] = NA
    end
  end
  DataFrame(columns, Index(names))
end


#
# TODO:
#  Find a way to avoid calls to cbind and rbind
#  Add methods that make first pass if no DataFrameMetaData is provided
#   that construct heuristic or perfect DataFrameMetaData

# Split line
# Convert items to specified types
# Create DataVec for each
# Create DataFrame from DataVecs
# Use colnames from DataFrameMetaData

function parse_delimited_line{T <: String}(line::T, delimiter::Char, quote_character::Char, md::DataFrameMetaData)
  items = split_delimited_line(line, delimiter, quote_character)

  (results, is_missing) = convert(md, items)

  n = length(results)

  if n != length(is_missing)
    error("Missingness information is corrupt")
  end

  df = DataFrame()
  for i in 1:n
    df = cbind(df, DataFrame(DataVec([results[i]], [is_missing[i]])))
  end

  if length(md.column_names) != ncol(df)
    colnames!(df, [strcat("x", i) for i in 1:ncol(df)])
  else
    colnames!(df, md.column_names)  
  end

  return df
end

# Open file
# Call parse_delimited_line() on each line
# rbind() all results into one DataFrame
# Return one DataFrame

function parse_delimited_file(filename::String, delimiter::Char, quote_character::Char, md::DataFrameMetaData)
  df = DataFrame()

  f = open(filename, "r")

  for line in readlines(f)
    line_df = parse_delimited_line(chomp(line), delimiter, quote_character, md)

    if nrow(df) == 0
      df = line_df
    else
      df = rbind(df, line_df)
    end
  end

  close(f)

  return df
end

function alt_parse_delimited_file(filename::String, delimiter::Char, quote_character::Char, md::DataFrameMetaData)
  f = open(filename, "r")

  dfs = Array(DataFrame, 0)

  for line in readlines(f)
    line_df = parse_delimited_line(chomp(line), delimiter, quote_character, md)

    push(dfs, line_df)
  end

  close(f)

  return reduce(rbind, dfs)

  # n = length(dfs)
  # p = ncol(dfs[1])

  # # Create DataFrame that is NA everywhere.
  # # Then fill in entries.

  # for i = 1:p
  #   DataVec{md.types[i]}(Array(md.types[i], n), Array(Bool, 5))
  # end

  # return df
end

function alt2_parse_delimited_file(filename::String, n::Int64, delimiter::Char, quote_character::Char, md::DataFrameMetaData)
  df = DataFrame(n, md)

  p = length(md.types)

  i = 0

  f = open(filename, "r")

  for line in readlines(f)
    #print(line)

    i += 1

    line_df = parse_delimited_line(chomp(line), delimiter, quote_character, md)

    for j in 1:p
      df[i, j] = line_df[1, j]
    end
  end

  close(f)

  return df
end
