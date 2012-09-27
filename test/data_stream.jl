load("src/data_stream.jl")

cds = CSVDataStream("test/data/complex_data.csv")

cds.metadata.column_names = ["A", "B", "C", "D", "E"]
cds.metadata.types = {String, String, String, Float64, Int64}

for row in cds
  println()
  println(row)
  println()
end

cds.metadata.types = {String, String, String, Float64, Float64}

for row in cds
  println()
  println(row)
  println()
end

f = open("test/data/big_data.csv", "w")

for i in 1:10_000
  println(f, join({"I'm A", "I'm B", "I'm C", randn(), randn() + 19.0}, ','))
end

close(f)

cds = CSVDataStream("test/data/big_data.csv")

cds.metadata.column_names = ["A", "B", "C", "D", "E"]
cds.metadata.types = {String, String, String, Float64, Float64}

@elapsed mean(cds)

file_remove("test/data/big_data.csv")
