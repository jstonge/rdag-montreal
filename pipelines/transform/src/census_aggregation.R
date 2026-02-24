# Census aggregation - pull demographic data from cancensus API
# Combines geometry + attributes for Montreal (2021 Census)
# Outputs both DA (Dissemination Area) and CT (Census Tract) levels
library(cancensus)
library(dplyr)
library(arrow)
library(here)
library(fs)

# Census vectors to pull — confirm IDs with:
#   find_census_vectors("topic", dataset = "CA21", query_type = "semantic")
# Convention: {metric}_{dimension}_{value}
CENSUS_VECTORS <- c(
  # Population
  "pop_total"                      = "v_CA21_1",
  # Age
  "pop_age_0to14"                  = "v_CA21_8",
  "pop_age_15to64"                 = "v_CA21_251",
  "pop_age_65plus"                 = "v_CA21_254",
  "avg_age_sex_total"              = "v_CA21_386",
  # Income

  ## median
  "median_income_household"                 = "v_CA21_906",
  "median_income_aftertax_household"        = "v_CA21_910",
  "median_income_total"   = "v_CA21_983",
  "median_income_male"    = "v_CA21_984",
  "median_income_female"  = "v_CA21_985",
  "median_income_aftertax_total"   = "v_CA21_986",
  "median_income_aftertax_male"    = "v_CA21_987",
  "median_income_aftertax_female"  = "v_CA21_988",
  
  ## average
  "avg_income_household"   = "v_CA21_915",
  "avg_income_aftertax_household"   = "v_CA21_916",
  "avg_total_income_total"   = "v_CA21_1004",
  "avg_total_income_male"    = "v_CA21_1005",
  "avg_total_income_female"  = "v_CA21_1006",
  # Housing
  "dwellings_total"                = "v_CA21_4",
  "tenure_owner"                   = "v_CA21_4239",
  "tenure_renter"                  = "v_CA21_4240",
  # Language
  "lang_mother_english"            = "v_CA21_1144",
  "lang_mother_french"             = "v_CA21_1147",
  # Immigration
  "pop_immigrant"                  = "v_CA21_4404"
)

# Rename cancensus columns (e.g. "v_CA21_986: Some label") to clean convention names
rename_census_columns <- function(df) {
  col_names <- names(df)
  for (i in seq_along(CENSUS_VECTORS)) {
    vec_id <- CENSUS_VECTORS[i]
    clean_name <- names(CENSUS_VECTORS)[i]
    match_idx <- grep(paste0("^", vec_id, ":"), col_names)
    if (length(match_idx) == 1) {
      col_names[match_idx] <- clean_name
    }
  }
  names(df) <- col_names
  df
}

# Rename default cancensus columns to snake_case
COLUMN_RENAMES <- c(
  "GeoUID"      = "geo_uid",
  "Type"        = "type",
  "Region Name" = "region_name",
  "Area (sq km)" = "area_sqkm",
  "Population"  = "population",
  "Dwellings"   = "dwellings",
  "Households"  = "households",
  "CSD_UID"     = "csd_uid",
  "CD_UID"      = "cd_uid",
  "CT_UID"      = "ct_uid",
  "CMA_UID"     = "cma_uid"
)

rename_default_columns <- function(df) {
  col_names <- names(df)
  for (old_name in names(COLUMN_RENAMES)) {
    idx <- match(old_name, col_names)
    if (!is.na(idx)) {
      col_names[idx] <- COLUMN_RENAMES[old_name]
    }
  }
  names(df) <- col_names
  df
}

fetch_census_level <- function(level) {
  get_census(
    dataset = "CA21",
    regions = list(CSD = "2466023"),
    vectors = unname(CENSUS_VECTORS),
    level = level,
    geo_format = "sf"
  ) |>
    rename_census_columns() |>
    rename_default_columns() |>
    sf::st_transform(crs = 4326) |>
    rmapshaper::ms_simplify(keep = 0.05, keep_shapes = TRUE)
}

write_census_outputs <- function(data, level_label) {
  output_dir <- here("pipelines", "transform", "input")
  dir_create(output_dir, recurse = TRUE)

  parquet_path <- file.path(output_dir, paste0("census_", level_label, ".parquet"))
  data |>
    sf::st_drop_geometry() |>
    filter(population > 0) |>
    write_parquet(parquet_path)
  message(sprintf("Wrote %d %ss to %s", nrow(data), toupper(level_label), parquet_path))

  parquet_path
}

census_aggregation <- function() {
  da_paths <- write_census_outputs(fetch_census_level("DA"), "da")
  ct_paths <- write_census_outputs(fetch_census_level("CT"), "ct")
  list(da = da_paths, ct = ct_paths)
}
