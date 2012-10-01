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
function ref(cds::DataStream, ind::Int64)
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

function colmeans(cds::DataStream)
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

mean(cds::DataStream) = colmeans(cds)

function var(cds::DataStream)
  p = length(cds.metadata.types)
  is_numeric = map(t -> t <: Number, cds.metadata.types)

  n = zeros(Int64, p)
  means = zeros(Float64, p)
  m2 = zeros(Float64, p)
  variances = zeros(Float64,p)

  for row in cds
    for j in 1:p
      if is_numeric[j] & !isna(row[1, j])
        n[j] += 1
        delta = row[1, j] - means[j]
        means[j] += delta / n[j]
        m2[j] += delta * (row[1, j] - means[j])
        variances[j] = m2[j] / (n[j] - 1)
      end
    end
  end

  df = DataFrame(1, p)
  names!(df, cds.metadata.column_names)

  for j = 1:p
    if !is_numeric[j]
      df[1, j] = NA
    else
      df[1, j] = variances[j]
    end
  end

  return df
end

function range(cds::DataStream)
  p = length(cds.metadata.types)
  is_numeric = map(t -> t <: Number, cds.metadata.types)

  mins = zeros(p) # Set to Inf
  maxs = zeros(p) # Set to -Inf

  for j in 1:p
    mins[j] = Inf
    maxs[j] = -Inf
  end

  for row in cds
    for j in 1:p
      if is_numeric[j] & !isna(row[1, j])
        if row[1, j] < mins[j]
          mins[j] = row[1, j]
        end
        if row[1, j] > maxs[j]
          maxs[j] = row[1, j]
        end
      end
    end
  end

  df = DataFrame(2, p)
  names!(df, cds.metadata.column_names)

  for j = 1:p
    if !is_numeric[j]
      df[1, j] = NA
      df[2, j] = NA
    else
      df[1, j] = mins[j]
      df[2, j] = maxs[j]
    end
  end

  return cbind(DataFrame(:(Type = ["Min", "Max"])), df)
end

# cov(cds): Returns a PxP matrix
# cor(cds): Returns a PxP matrix
# entropy(cds): Returns a 1xP DataFrame
# lm(cds, Formula)
# glm(cds, Formula)
# k_means(cds, Formula)
# svm(cds, Formula)
