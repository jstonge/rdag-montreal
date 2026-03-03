source(here::here("pipelines", "transform", "src", "wranglers", "utils.R"))

# Same hybrid strategy as 2006: StatCan for demerged municipalities,
# Ville de Montréal for arrondissements.

wrangle_2011_statcan <- function(input_dir, metadata_layer) {
  dir_sc <- here::here(input_dir, metadata_layer, "2011_statcan")
  csv_files <- fs::dir_ls(dir_sc, glob = "*.csv")

  rows <- lapply(csv_files, function(f) {
    line1 <- readr::read_lines(f, n_max = 1, locale = readr::locale(encoding = "latin1"))
    name <- gsub(",.*", "", gsub("^,,,", "", line1))
    name <- sub(" - .*", "", name)

    df <- readr::read_csv(f, skip = 1, show_col_types = FALSE, name_repair = "minimal",
                          locale = readr::locale(encoding = "latin1"))
    pop_row <- df[df$Characteristics == "Population in 2011", ]
    pop <- as.numeric(trimws(as.character(pop_row[[4]][1])))

    dplyr::tibble(arrondissement = name, population = pop)
  })

  dplyr::bind_rows(rows) |>
    dplyr::filter(!arrondissement %in% c("Montr\u00e9al")) |>
    dplyr::mutate(
      arrondissement = normalize_name(arrondissement),
      year = 2011L,
      source = "StatCan 2011 Census profiles"
    )
}

wrangle_2011_mtl <- function(input_dir, metadata_layer, fname) {
  file_path <- here::here(input_dir, metadata_layer, "2011", fname)

  readr::read_csv(file_path, skip = 2, show_col_types = FALSE) |>
    dplyr::slice(1:(dplyr::n() - 4)) |>
    dplyr::rename(arrondissement = 1) |>
    dplyr::select(arrondissement, population = `Population en 2011`) |>
    dplyr::filter(!arrondissement %in% c("Autres villes", "Ville de Montr\u00e9al",
                                          "AGGLOM\u00c9RATION DE MONTR\u00c9AL")) |>
    dplyr::mutate(
      arrondissement = normalize_name(arrondissement),
      year = 2011L,
      population = readr::parse_number(as.character(population)),
      source = "Ville de Montr\u00e9al"
    )
}

wrangle_2011 <- function(input_dir, metadata_layer, fname) {
  sc <- wrangle_2011_statcan(input_dir, metadata_layer)
  mtl <- wrangle_2011_mtl(input_dir, metadata_layer, fname)

  # Keep arrondissements from Ville de Montréal, demerged municipalities from StatCan
  arrondissements <- mtl |> dplyr::filter(!arrondissement %in% sc$arrondissement)
  dplyr::bind_rows(sc, arrondissements)
}
