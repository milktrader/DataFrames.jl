# A simple two-state machine that splits delimited lines on `dlm`
# but ignores dlm when inside a region bounded by `quote_character`.
function split_delimited_line{T <: String}(line::T, dlm::Char, quote_character::Char)
  inside_quotes = false
  items = Array(String, 0)
  current_item = ""
  for chr in line
    if inside_quotes
      if chr != quote_character
        current_item = current_item * string(chr)
      else
        inside_quotes = false
      end
    else
      if chr == quote_character
        inside_quotes = true
      else
        if chr == dlm
          push(items, current_item)
          current_item = ""
        else
          current_item = current_item * string(chr)
        end
      end
    end
  end
  push(items, current_item)
  return items
end
