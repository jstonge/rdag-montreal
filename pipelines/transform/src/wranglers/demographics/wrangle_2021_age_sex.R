source(here::here("pipelines", "transform", "src", "wranglers", "utils.R"))

# Extract age x sex cross-tabulation from 2021 census data.
# Hybrid approach: StatCan bulk CSV for demerged municipalities (CSDs),
# Ville de Montréal census profile for arrondissements.
#
# Returns long format: (arrondissement, year, age_group, sex, population, source)

# Characteristic ID -> age group label mapping for 2021 StatCan bulk CSV.
# These are the leaf-level 5-year brackets (not summary groups like "0 to 14").
AGE_CHAR_MAP <- c(
  "10" = "0-4",   "11" = "5-9",   "12" = "10-14",
  "14" = "15-19", "15" = "20-24", "16" = "25-29",
  "17" = "30-34", "18" = "35-39", "19" = "40-44",
  "20" = "45-49", "21" = "50-54", "22" = "55-59",
  "23" = "60-64", "25" = "65-69", "26" = "70-74",
  "27" = "75-79", "28" = "80-84", "30" = "85-89",
  "31" = "90-94", "32" = "95-99", "33" = "100+"
)

# --- StatCan: demerged municipalities (per-CSD data) -------------------------

wrangle_2021_age_sex_statcan <- function(input_dir, metadata_layer) {
  file_path <- here::here(input_dir, metadata_layer, "2021_statcan", "montreal_census_2021.csv")
  df <- readr::read_csv(file_path, show_col_types = FALSE)

  age_df <- df |>
    dplyr::filter(CHARACTERISTIC_ID %in% as.integer(names(AGE_CHAR_MAP))) |>
    dplyr::mutate(
      arrondissement = sub(",.*$", "", GEO_NAME) |> normalize_name(),
      age_group = AGE_CHAR_MAP[as.character(CHARACTERISTIC_ID)]
    ) |>
    dplyr::filter(!arrondissement %in% c("Montr\u00e9al"))

  # Pivot total/men/women columns into long sex rows
  total  <- age_df |> dplyr::transmute(arrondissement, age_group, sex = "total",  population = C1_COUNT_TOTAL)
  male   <- age_df |> dplyr::transmute(arrondissement, age_group, sex = "male",   population = `C2_COUNT_MEN+`)
  female <- age_df |> dplyr::transmute(arrondissement, age_group, sex = "female", population = `C3_COUNT_WOMEN+`)

  dplyr::bind_rows(total, male, female) |>
    dplyr::mutate(year = 2021L, source = "StatCan 2021 Census profiles")
}

# --- Ville de Montréal: arrondissements (sub-CSD data) -----------------------

# French age labels in the Ville de Montréal CSV -> standardised English labels
AGE_LABELS_FR_TO_EN <- c(
  "0 \u00e0 4 ans"   = "0-4",   "5 \u00e0 9 ans"   = "5-9",   "10 \u00e0 14 ans" = "10-14",
  "15 \u00e0 19 ans" = "15-19", "20 \u00e0 24 ans" = "20-24", "25 \u00e0 29 ans" = "25-29",
  "30 \u00e0 34 ans" = "30-34", "35 \u00e0 39 ans" = "35-39", "40 \u00e0 44 ans" = "40-44",
  "45 \u00e0 49 ans" = "45-49", "50 \u00e0 54 ans" = "50-54", "55 \u00e0 59 ans" = "55-59",
  "60 \u00e0 64 ans" = "60-64", "65 \u00e0 69 ans" = "65-69", "70 \u00e0 74 ans" = "70-74",
  "75 \u00e0 79 ans" = "75-79", "80 \u00e0 84 ans" = "80-84", "85 \u00e0 89 ans" = "85-89",
  "90 \u00e0 94 ans" = "90-94", "95 \u00e0 99 ans" = "95-99", "100 ans et plus"  = "100+"
)

clean_mtl_arrondissement <- function(name) {
  name |>
    (\(x) dplyr::case_when(
      stringr::str_starts(x, "Arrondissement du ") ~
        paste0("Le ", stringr::str_remove(x, "^Arrondissement du ")),
      TRUE ~ stringr::str_remove(x, "^Arrondissement d[e']\u2019?\\s*")
    ))() |>
    stringr::str_remove("^Ville de\\s*") |>
    stringr::str_remove("^Village de\\s*") |>
    normalize_name()
}

# Parse one age-by-sex block from the wide Ville de Montréal CSV
extract_age_block <- function(raw, section_label, sex, count_cols, arrondissements) {
  labels <- raw[[1]]
  header_idx <- which(labels == section_label)
  if (length(header_idx) == 0) stop("Section not found: ", section_label)

  section <- raw[(header_idx + 1):min(header_idx + 30, nrow(raw)), ]
  section_labels <- section[[1]]

  rows <- list()
  for (age_fr in names(AGE_LABELS_FR_TO_EN)) {
    idx <- which(section_labels == age_fr)
    if (length(idx) == 0) next
    pops <- as.numeric(section[idx[1], count_cols])
    rows[[length(rows) + 1]] <- dplyr::tibble(
      arrondissement = arrondissements,
      age_group = AGE_LABELS_FR_TO_EN[age_fr],
      sex = sex,
      population = pops
    )
  }
  dplyr::bind_rows(rows)
}

wrangle_2021_age_sex_mtl <- function(input_dir, metadata_layer, fname) {
  file_path <- here::here(input_dir, metadata_layer, "2021", fname)
  raw <- readr::read_csv(file_path, skip = 3, show_col_types = FALSE)

  count_cols <- seq(2, ncol(raw), by = 2)
  arrondissements <- names(raw)[count_cols] |> clean_mtl_arrondissement()

  total  <- extract_age_block(raw, "Population totale selon le groupe d'\u00e2ges",  "total",  count_cols, arrondissements)
  male   <- extract_age_block(raw, "Hommes + selon le groupe d'\u00e2ges",           "male",   count_cols, arrondissements)
  female <- extract_age_block(raw, "Femmes + selon le groupe d'\u00e2ges",           "female", count_cols, arrondissements)

  exclude <- c(
    clean_mtl_arrondissement("AGGLOM\u00c9RATION DE MONTR\u00c9AL"),
    clean_mtl_arrondissement("Ville de Montr\u00e9al"),
    "Montr\u00e9al"
  ) |> unique()

  dplyr::bind_rows(total, male, female) |>
    dplyr::filter(!arrondissement %in% exclude) |>
    dplyr::mutate(year = 2021L, source = "Ville de Montr\u00e9al")
}

# --- Combined: StatCan for demerged, Ville de Montréal for arrondissements ----

wrangle_2021_age_sex <- function(input_dir, metadata_layer, fname) {
  sc  <- wrangle_2021_age_sex_statcan(input_dir, metadata_layer)
  mtl <- wrangle_2021_age_sex_mtl(input_dir, metadata_layer, fname)

  # Keep only Ville de Montréal rows for districts not in StatCan (arrondissements)
  arrondissements <- mtl |> dplyr::filter(!arrondissement %in% unique(sc$arrondissement))
  dplyr::bind_rows(sc, arrondissements)
}
