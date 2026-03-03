source(here::here("pipelines", "transform", "src", "wranglers", "utils.R"))
source(here::here("pipelines", "transform", "src", "wranglers", "demographics", "wrangle_2021_age_sex.R"))

# Extract age x sex cross-tabulation from 2016 StatCan bulk census profile.
# Same hybrid approach as 2021: StatCan for demerged municipalities,
# Ville de Montréal for arrondissements.
#
# The 2016 bulk CSV has different column names than 2021 but the same
# characteristic IDs for age groups. AGE_CHAR_MAP is shared from wrangle_2021_age_sex.R.

# 2016 column name mapping
COL_CHAR_ID_2016  <- "Member ID: Profile of Census Subdivisions (2247)"
COL_CHAR_NAME_2016 <- "DIM: Profile of Census Subdivisions (2247)"
COL_TOTAL_2016    <- "Dim: Sex (3): Member ID: [1]: Total - Sex"
COL_MALE_2016     <- "Dim: Sex (3): Member ID: [2]: Male"
COL_FEMALE_2016   <- "Dim: Sex (3): Member ID: [3]: Female"

# --- StatCan: demerged municipalities ----------------------------------------

wrangle_2016_age_sex_statcan <- function(input_dir, metadata_layer) {
  file_path <- here::here(input_dir, metadata_layer, "2016_statcan", "montreal_census_2016.csv")
  df <- readr::read_csv(file_path, show_col_types = FALSE)

  age_df <- df |>
    dplyr::filter(.data[[COL_CHAR_ID_2016]] %in% as.integer(names(AGE_CHAR_MAP))) |>
    dplyr::mutate(
      arrondissement = normalize_name(GEO_NAME),
      age_group = AGE_CHAR_MAP[as.character(.data[[COL_CHAR_ID_2016]])]
    ) |>
    dplyr::filter(!arrondissement %in% c("Montr\u00e9al"))

  total  <- age_df |> dplyr::transmute(arrondissement, age_group, sex = "total",
                                        population = as.numeric(.data[[COL_TOTAL_2016]]))
  male   <- age_df |> dplyr::transmute(arrondissement, age_group, sex = "male",
                                        population = as.numeric(.data[[COL_MALE_2016]]))
  female <- age_df |> dplyr::transmute(arrondissement, age_group, sex = "female",
                                        population = as.numeric(.data[[COL_FEMALE_2016]]))

  dplyr::bind_rows(total, male, female) |>
    dplyr::mutate(year = 2016L, source = "StatCan 2016 Census profiles")
}

# --- Ville de Montréal: arrondissements --------------------------------------
# The 2016 XLS has age and sex in separate sheets (not cross-tabulated like 2021).
# Sheet "02_Groupes d'âge, âge moyen" gives total population by age group per district.
# We can only extract sex = "total" (no male/female split per age group).

wrangle_2016_age_sex_mtl <- function(input_dir, metadata_layer, fname) {
  file_path <- here::here(input_dir, metadata_layer, "2016", "age_mtl_by_district.csv")
  if (!file.exists(file_path)) {
    return(dplyr::tibble(
      arrondissement = character(), age_group = character(),
      sex = character(), population = numeric(),
      year = integer(), source = character()
    ))
  }

  raw <- readr::read_csv(file_path, show_col_types = FALSE)

  # Row 2 has age group labels in even-numbered columns (count columns).
  # Odd columns (after col 1) are percentages.
  age_label_row <- raw[2, ]

  count_cols <- c()
  age_groups <- c()
  for (j in seq_along(raw)) {
    label <- as.character(age_label_row[[j]])
    if (!is.na(label) && label %in% names(AGE_LABELS_FR_TO_EN)) {
      count_cols <- c(count_cols, j)
      age_groups <- c(age_groups, AGE_LABELS_FR_TO_EN[label])
    }
  }

  # Data starts at row 3 (after blank row and age-labels row)
  data <- raw[3:nrow(raw), ]
  districts <- as.character(data[[1]])

  # Drop aggregate rows and footer
  keep <- !is.na(districts) &
    !grepl("^(Source:|Retour|AGGLOM)", districts) &
    districts != "Ville de Montr\u00e9al" &
    districts != "Autres villes"
  data <- data[keep, ]
  districts <- districts[keep]

  rows <- list()
  for (i in seq_along(districts)) {
    for (k in seq_along(count_cols)) {
      pop <- suppressWarnings(as.numeric(data[[count_cols[k]]][i]))
      rows[[length(rows) + 1]] <- dplyr::tibble(
        arrondissement = normalize_name(districts[i]),
        age_group = age_groups[k],
        sex = "total",
        population = pop,
        year = 2016L,
        source = "Ville de Montr\u00e9al"
      )
    }
  }

  dplyr::bind_rows(rows)
}

# --- Combined ----------------------------------------------------------------

wrangle_2016_age_sex <- function(input_dir, metadata_layer, fname) {
  sc  <- wrangle_2016_age_sex_statcan(input_dir, metadata_layer)
  mtl <- wrangle_2016_age_sex_mtl(input_dir, metadata_layer, fname)

  arrondissements <- mtl |> dplyr::filter(!arrondissement %in% unique(sc$arrondissement))
  dplyr::bind_rows(sc, arrondissements)
}
