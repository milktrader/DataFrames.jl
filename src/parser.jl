load("src/split.jl")
load("src/convert.jl")

load("src/init.jl")

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

