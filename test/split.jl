load("src/split.jl")

delimiters = [',', '\t', ' ']
quote_characters = ['\'', '"']

# TODO: Test minimially-quoted
# TODO: Test only-strings-quoted

# Test all-entries-quoted for all quote characters and delimiters
items = {"a", "b", "c,d", "1.0", "1"}

for delimiter in delimiters
  for quote_character in quote_characters
    line = join(map(x -> strcat(quote_character, x, quote_character), items), delimiter)

    split_results = split_delimited_line(line, delimiter, quote_character)

    @assert all(split_results .== items)
  end
end
