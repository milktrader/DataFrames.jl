# Load DataFrame package.
load("src/init.jl")
load("src/svd.jl")

# Load voting data from a CSV file into a DataFrame.
df = csvDataFrame("demo/senate112.csv")

# Separate out the names of Senators for later analysis.
senator_names = df[:, 425]

# Remove the names to make a de facto DataMatrix.
df = df[:, 1:424]

# Run the missing-data SVD in 2 dimensions.
imputed_df, u, d, v = missing_svd(df, 2)

# Estimate latent positions of U.S. Senators in 2D.
u = u * sqrt(diagm(d))

# Store the imputed latent positions in a TSV file.
f = open("demo/ideal_points.tsv", "w")
for i = 1:nrow(df)
  println(f, join([senator_names[i], string(u[i, 1]), string(u[i, 2])], "\t"))
end
close(f)
