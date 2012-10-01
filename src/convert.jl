type DataFrameMetaData
  column_names::Vector{ASCIIString}
  types::Array
  #types::Vector{Union(BitsKind,CompositeKind,AbstractKind)}
  #types::Array{Type}
end

missing_value_defaults = Dict()
missing_value_defaults[Float64] = 0.0
missing_value_defaults[Int64] = 0
missing_value_defaults[ASCIIString] = ""
missing_value_defaults[String] = ""

function convert{T <: String}(md::DataFrameMetaData, items::Array{T})
  p = length(items)

  results = Array(Any, p)
  is_missing = Array(Bool, p)

  if length(md.types) != p
    error("Length of inputs to convert() do not match")
  end

  for i in 1:p
    if length(items[i]) == 0
      is_missing[i] = true
      results[i] = missing_value_defaults[md.types[i]]
    else
      is_missing[i] = false
      if md.types[i] == Float64
        results[i] = float(items[i])
      elseif md.types[i] == Int64
        results[i] = int(items[i])    
      else
        results[i] = convert(md.types[i], items[i])
      end
    end
  end

  return (results, is_missing)
end
