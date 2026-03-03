source(here::here("pipelines", "transform", "src", "wranglers", "utils.R"))

wrangle_2001 <- function(input_dir, metadata_layer) {
  dir_2001 <- here::here(input_dir, metadata_layer, "2001_statcan")
  csv_files <- fs::dir_ls(dir_2001, glob = "*.csv")

  rows <- lapply(csv_files, function(f) {
    lines <- readr::read_lines(f, n_max = 4, locale = readr::locale(encoding = "latin1"))
    # Row 2: "Geography = <name> [1]"
    # Row 3: "Population, 2001 - 100% Data [1]",<number>
    name <- gsub('^\"|\"$', "", lines[2])
    name <- sub("^Geography = ", "", name)
    name <- sub("\\s*\\[\\d+\\]$", "", name)
    pop <- as.numeric(sub('^"Population, 2001.*",', "", lines[3]))

    dplyr::tibble(
      arrondissement = name,
      population = pop,
      year = 2001L
    )
  })

  dplyr::bind_rows(rows) |>
    dplyr::filter(arrondissement != "Montr\u00e9al") |>
    dplyr::mutate(arrondissement = normalize_name(arrondissement))
}
