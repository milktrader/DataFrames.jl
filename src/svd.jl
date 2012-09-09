# Calculates the SVD of a data matrix containing missing entries.
#
# This should really be done with a DataMatrix{Int64} or a 
# DataMatrix{Float64}, but it's currently being done with generic
# DataFrame's that should be edited in advance to insure that the
# algorithm won't crash.

# Uses the iterative algorithm of Hastie et al. 1999

# Calculate the rank-k SVD approximation to a matrix given the
# full SVD.
approximate(u::Array{Float64},
            d::Array{Float64},
            v::Array{Float64},
            k::Int64) = u[:, 1:k] * diagm(d[1:k]) * v[1:k, :]

# Test code:
#
# srand(1)
# M = rand(3, 3)
# (u, d, v) = svd(M)
# norm(approximate(u, d, v, 1) - M)
# norm(approximate(u, d, v, 2) - M)
# norm(approximate(u, d, v, 3) - M)

# Impute a missing entries using current approximation.
function impute(m::Matrix{Float64},
                missing_entries::Array{Any},
                u::Array{Float64},
                d::Array{Float64},
                v::Array{Float64},
                k::Int64)
  approximate_m = approximate(u, d, v, k)

  for indices in missing_entries
    m[indices[1], indices[2]] = approximate_m[indices[1], indices[2]]
  end

  m
end

# Should be done with a proper N-dimensional Int64 array.
function ind_na(df::DataFrame)
  indices = {}
  for i = 1:nrow(df)
    for j = 1:ncol(df)
      if isna(df[i, j])
        push(indices, [i, j])
      end
    end
  end
  indices
end

# Kind of a nutty method without constraint that we're using a DataMatrix.
function mean(df::DataFrame)
  mu = 0.0
  n = 0
  for i = 1:nrow(df)
    for j = 1:ncol(df)
      if !isna(df[i, j])
        mu += df[i, j]
        n += 1
      end
    end
  end
  mu / n
end

# Calculate the mean of each row in a DataFrame.
function row_means(df::DataFrame)
  means = zeros(nrow(df))
  for i = 1:nrow(df)
    mu = 0.0
    n = 0
    for j = 1:ncol(df)
      if !isna(df[i, j])
        mu += df[i, j]
        n += 1
      end
    end
    if n == 0
      error("Row $i has all NA entries")
    end
    means[i] = mu / n
  end
  means
end

# Calculate the mean of each column in a DataFrame.
function col_means(df::DataFrame)
  means = zeros(ncol(df))
  for j = 1:ncol(df)
    mu = 0.0
    n = 0
    for i = 1:nrow(df)
      if !isna(df[i, j])
        mu += df[i, j]
        n += 1
      end
    end
    if n == 0
      error("Column $j has all NA entries")
    end
    means[j] = mu / n
  end
  means
end

# Uses a fixed point algorithm to calculate the SVD of a DataFrame
# that may be missing entries. Should really be defined on a 
# DataMatrix type, not a DataFrame type.
function missing_svd(D::DataFrame, k::Int)
  # Don't edit the original DataFrame.
  df = copy(D)

  # Select a tolerance before diagnosing convergence.
  tolerance = 10e-4

  # Store the dimensions of the matrix in variables.
  n = size(df, 1)
  p = size(df, 2)

  # Estimate the missingness of the DataFrame and print a message.
  missing_entries = ind_na(df)
  missingness = length(missing_entries) / (n * p)
  println("Matrix is missing $(missingness * 100)% of entries")

  # Initial imputation uses row means where possible and the global
  # mean otherwise.
  global_mu = mean(df)
  mu_i = row_means(df)
  for i = 1:n
    for j = 1:p
      if isna(df[i, j])
        if isna(mu_i[i])
          df[i, j] = global_mu
        else
          df[i, j] = mu_i[i]          
        end
      end
    end
  end

  # Now we make a Float64 matrix out of the fully filled-in DataFrame.
  tmp = zeros(n, p)
  for i = 1:n
    for j = 1:p
      tmp[i, j] = df[i, j]
    end
  end

  # Count iterations of proper imputation method.
  i = 0

  # Keep track of approximate matrices.
  previous_m = tmp
  current_m = copy(previous_m)

  # Keep track of Frobenius norm of changes in imputed matrix.
  change = Inf

  # Iterate until imputation stops changing up to chosen tolerance.
  while change > tolerance
    # Print out status of algorithm for monitoring convergence.
    println("Iteration: $i")
    println("Change in Frobenius Norm: $change")

    # Make copies of matrices.
    previous_m = current_m
    current_m = copy(current_m)

    # Re-impute missing entries using the SVD of the current impuation.
    u, d, v = svd(current_m)
    current_m = impute(current_m, missing_entries, u, d, v, k)

    # Compute the change in the matrix across iterations.
    change = norm(previous_m - current_m) / norm(tmp)

    # Increment the iteration counter.
    i = i + 1
  end

  # Tell the user how many iterations were required to impute matrix.
  println("Tolerance achieved after $i iterations")

  # Return both df and the SVD of df with all entries imputed.
  # Need to check that v is being used correctly.
  u, d, v = svd(current_m)
  (current_m, u[:, 1:k], d[1:k], v[1:k, :])
end
