source(here::here("pipelines", "transform", "src", "wranglers", "utils.R"))

# Same hybrid strategy: StatCan for demerged municipalities, Ville de Montréal
# for arrondissements.

wrangle_2016_statcan <- function(input_dir, metadata_layer) {
  file_path <- here::here(input_dir, metadata_layer, "2016_statcan", "montreal_population_2016.csv")
  df <- readr::read_csv(file_path, show_col_types = FALSE)

  # 2016 bulk CSV: col 4 = GEO_NAME, col 13 = Total population
  dplyr::tibble(
    arrondissement = df$GEO_NAME,
    population = df[[13]],
    year = 2016L,
    source = "StatCan 2016 Census profiles"
  ) |>
    dplyr::filter(!arrondissement %in% c("Montr\u00e9al")) |>
    dplyr::mutate(arrondissement = normalize_name(arrondissement))
}

wrangle_2016_mtl <- function(input_dir, metadata_layer, fname) {
  file_path <- here::here(input_dir, metadata_layer, "2016", fname)

  readr::read_csv(file_path, skip = 2, show_col_types = FALSE) |>
    dplyr::slice(1:(dplyr::n() - 4)) |>
    dplyr::rename(arrondissement = 1) |>
    dplyr::select(arrondissement, population = `Population en 2016`) |>
    dplyr::filter(!arrondissement %in% c("Autres villes", "Ville de Montr\u00e9al",
                                          "AGGLOM\u00c9RATION DE MONTR\u00c9AL")) |>
    dplyr::mutate(
      arrondissement = normalize_name(arrondissement),
      year = 2016L,
      population = readr::parse_number(as.character(population)),
      source = "Ville de Montr\u00e9al"
    )
}

wrangle_2016 <- function(input_dir, metadata_layer, fname) {
  sc <- wrangle_2016_statcan(input_dir, metadata_layer)
  mtl <- wrangle_2016_mtl(input_dir, metadata_layer, fname)

  arrondissements <- mtl |> dplyr::filter(!arrondissement %in% sc$arrondissement)
  dplyr::bind_rows(sc, arrondissements)
}
