normalize_name <- function(name) {
  name |>
    stringr::str_replace_all("\u2013", "-") |>
    stringr::str_replace_all("\u2019", "'")
}
