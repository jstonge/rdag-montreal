source(here::here("pipelines", "transform", "src", "wranglers", "utils.R"))

wrangle_1991 <- function(input_dir, metadata_layer) {
  dir_1991 <- here::here(input_dir, metadata_layer, "1991")
  csv_files <- fs::dir_ls(dir_1991, glob = "*.csv")

  rows <- lapply(csv_files, function(f) {
    lines <- readr::read_lines(f, n_max = 5, locale = readr::locale(encoding = "latin1"))
    # Row 2: "Geography = <name>"
    # Row 4: "Population, 1991 (2)",<number>
    name <- sub('^"Geography = (.*)"$', "\\1", lines[2])
    pop_line <- lines[4]
    pop <- as.numeric(sub('^"Population, 1991 \\(2\\)",', "", pop_line))

    dplyr::tibble(
      arrondissement = name,
      population = pop,
      year = 1991L
    )
  })

  df <- dplyr::bind_rows(rows) |>
    dplyr::filter(arrondissement != "Montr\u00e9al") |>
    dplyr::mutate(
      # Strip footnote markers like " [1]"
      arrondissement = stringr::str_remove(arrondissement, "\\s*\\[\\d+\\]$"),
      arrondissement = normalize_name(arrondissement)
    )

  df
}
