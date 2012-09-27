load("src/parser.jl")

#
# Abstract Type
#

abstract DataStream

#
# Concrete Types
#

type CSVDataStream <: DataStream
  filename::String
  stream::IOStream
  minibatch_size::Int
  metadata::DataFrameMetaData
end

#
# CSV
#

function CSVDataStream(filename::String, minibatch_size::Int)
  CSVDataStream(filename, open(filename, "r"), minibatch_size, DataFrameMetaData(Array(String, 0), Array(Any, 0)))
end

function CSVDataStream(filename::String)
  CSVDataStream(filename, open(filename, "r"), 1, DataFrameMetaData(Array(String, 0), Array(Any, 0)))
end

function start(cds::CSVDataStream)
  cds.stream = open(cds.filename, "r")
  seek(cds.stream, 0)
  readline(cds.stream)
end

function next(cds::CSVDataStream, line::String)
  (parse_delimited_line(chomp(line), ',', '"', cds.metadata), readline(cds.stream))
end

function done(cds::CSVDataStream, line::String)
  if !isempty(line)
    return false
  else
    close(cds.stream)
    return true
  end
end

# TODO: Don't go past end of stream.
function ref(cds::CSVDataStream, ind::Int64)
  i = 1
  line = start(cds)
  row = DataFrame()
  while i != ind + 1 && !done(cds, line)
    (row, line) = next(cds, line)
    i += 1
  end
  return row
end

#
# Functions
#

function colmeans(cds::CSVDataStream)
  n = length(cds.metadata.types)
  is_numeric = map(t -> t <: Number, cds.metadata.types)
  sums = DataVec(zeros(n))
  counts = zeros(Int, n)
  for row in cds
    for i in 1:n
      if is_numeric[i] && !isna(row[1, i])
        sums[i] += row[1, i]
        counts[i] += 1
      end
    end
  end
  for i in 1:n
    if is_numeric[i]
      sums[i] /= counts[i]
    end
  end
  df = DataFrame()
  for i = 1:n
    df = cbind(df, DataFrame(DataVec([sums[i]])))
  end
  colnames!(df, cds.metadata.column_names)
  for i = 1:n
    if !is_numeric[i]
      df[1, i] = NA
    end
  end
  df
end

mean(cds::CSVDataStream) = colmeans(cds)

