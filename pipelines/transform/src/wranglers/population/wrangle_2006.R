source(here::here("pipelines", "transform", "src", "wranglers", "utils.R"))

# StatCan per-CSD data covers only the 15 demerged municipalities.
# Ville de Montréal covers all 34 districts (19 arrondissements + 15 demerged).
# We use StatCan for the demerged municipalities and Ville de Montréal for the
# arrondissements, combining both into a single dataset.

wrangle_2006_statcan <- function(input_dir, metadata_layer) {
  dir_sc <- here::here(input_dir, metadata_layer, "2006_statcan")
  csv_files <- fs::dir_ls(dir_sc, glob = "*.csv")

  rows <- lapply(csv_files, function(f) {
    lines <- readr::read_lines(f, n_max = 5, locale = readr::locale(encoding = "latin1"))
    name <- gsub('^\"|\"$', "", lines[2])
    name <- sub("^Geography = ", "", name)
    name <- sub("\\s*\\[\\d+\\]$", "", name)
    pop <- as.numeric(sub('^"Population, 2006.*",', "", lines[4]))
    dplyr::tibble(arrondissement = name, population = pop)
  })

  dplyr::bind_rows(rows) |>
    dplyr::filter(!arrondissement %in% c("Montr\u00e9al", "Montr\u00e9al (CD)")) |>
    dplyr::mutate(
      arrondissement = normalize_name(arrondissement),
      year = 2006L,
      source = "StatCan 2006 Census profiles"
    )
}

wrangle_2006_mtl <- function(input_dir, metadata_layer, fname) {
  file_path <- here::here(input_dir, metadata_layer, "2006", fname)

  readr::read_csv(file_path, skip = 2, show_col_types = FALSE) |>
    dplyr::rename(arrondissement = 1) |>
    dplyr::select(arrondissement, population = 3) |>
    dplyr::filter(
      !is.na(arrondissement),
      !arrondissement %in% c("Autres villes", "Ville de Montr\u00e9al",
                              "AGGLOM\u00c9RATION DE MONTR\u00c9AL",
                              "Source : Statistique Canada, Recensement de 2006")
    ) |>
    dplyr::mutate(
      arrondissement = normalize_name(arrondissement),
      year = 2006L,
      population = readr::parse_number(as.character(population)),
      source = "Ville de Montr\u00e9al"
    )
}

wrangle_2006 <- function(input_dir, metadata_layer, fname) {
  sc <- wrangle_2006_statcan(input_dir, metadata_layer)
  mtl <- wrangle_2006_mtl(input_dir, metadata_layer, fname)

  # Keep arrondissements from Ville de Montréal, demerged municipalities from StatCan
  arrondissements <- mtl |> dplyr::filter(!arrondissement %in% sc$arrondissement)
  dplyr::bind_rows(sc, arrondissements)
}
