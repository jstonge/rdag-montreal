source(here::here("pipelines", "transform", "src", "wranglers", "utils.R"))

wrangle_1996 <- function(input_dir, metadata_layer) {
  dir_1996 <- here::here(input_dir, metadata_layer, "1996")
  csv_files <- fs::dir_ls(dir_1996, glob = "*.csv")

  rows <- lapply(csv_files, function(f) {
    lines <- readr::read_lines(f, n_max = 5, locale = readr::locale(encoding = "latin1"))
    # Row 2: "Geography = <name>"
    # Row 4: "Population, 1996 (100% data) [3]",<number>
    name <- sub('^"Geography = (.*)"$', "\\1", lines[2])
    pop <- as.numeric(sub('^"Population, 1996.*",', "", lines[4]))

    dplyr::tibble(
      arrondissement = name,
      population = pop,
      year = 1996L
    )
  })

  df <- dplyr::bind_rows(rows) |>
    dplyr::filter(arrondissement != "Montr\u00e9al") |>
    dplyr::mutate(
      arrondissement = stringr::str_remove(arrondissement, "\\s*\\[\\d+\\]$"),
      arrondissement = normalize_name(arrondissement)
    )

  df
}
