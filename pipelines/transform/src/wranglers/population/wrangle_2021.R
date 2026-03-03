source(here::here("pipelines", "transform", "src", "wranglers", "utils.R"))

# Same hybrid strategy: StatCan for demerged municipalities, Ville de Montréal
# for arrondissements.

wrangle_2021_statcan <- function(input_dir, metadata_layer) {
  file_path <- here::here(input_dir, metadata_layer, "2021_statcan", "montreal_population_2021.csv")
  df <- readr::read_csv(file_path, show_col_types = FALSE)

  # 2021 bulk CSV: GEO_NAME has CSD type suffix, e.g. "Westmount, Ville (V)"
  # C1_COUNT_TOTAL is the population column
  dplyr::tibble(
    arrondissement = sub(",.*$", "", df$GEO_NAME),
    population = df$C1_COUNT_TOTAL,
    year = 2021L,
    source = "StatCan 2021 Census profiles"
  ) |>
    dplyr::filter(!arrondissement %in% c("Montr\u00e9al")) |>
    dplyr::mutate(arrondissement = normalize_name(arrondissement))
}

wrangle_2021_mtl <- function(input_dir, metadata_layer, fname) {
  file_path <- here::here(input_dir, metadata_layer, "2021", fname)

  raw <- readr::read_csv(file_path, skip = 3, show_col_types = FALSE)
  pop_row <- raw |> dplyr::filter(dplyr::if_any(1, ~ . == "Population totale en 2021"))
  col_names <- names(raw)
  count_cols <- seq(2, ncol(raw), by = 2)

  dplyr::tibble(
    arrondissement = col_names[count_cols],
    population = as.numeric(pop_row[1, count_cols]),
    year = 2021L,
    source = "Ville de Montr\u00e9al"
  ) |>
    dplyr::filter(
      !arrondissement %in% c("AGGLOM\u00c9RATION DE MONTR\u00c9AL", "Ville de Montr\u00e9al")
    ) |>
    dplyr::mutate(
      arrondissement = dplyr::case_when(
        stringr::str_starts(arrondissement, "Arrondissement du ") ~
          paste0("Le ", stringr::str_remove(arrondissement, "^Arrondissement du ")),
        TRUE ~ stringr::str_remove(arrondissement, "^Arrondissement d[e']\\s*")
      ),
      arrondissement = stringr::str_remove(arrondissement, "^Ville de\\s*"),
      arrondissement = stringr::str_remove(arrondissement, "^Village de\\s*"),
      arrondissement = normalize_name(arrondissement)
    )
}

wrangle_2021 <- function(input_dir, metadata_layer, fname) {
  sc <- wrangle_2021_statcan(input_dir, metadata_layer)
  mtl <- wrangle_2021_mtl(input_dir, metadata_layer, fname)

  arrondissements <- mtl |> dplyr::filter(!arrondissement %in% sc$arrondissement)
  dplyr::bind_rows(sc, arrondissements)
}
