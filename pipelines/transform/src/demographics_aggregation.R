# Demographics aggregation - combine age x sex cross-tabulations across census years
# Outputs census_population.parquet: (arrondissement, year, age_group, sex, population, source)
library(dplyr)
library(here)
library(fs)
library(arrow)

source(here("pipelines", "transform", "src", "wranglers", "demographics", "wrangle_2016_age_sex.R"))
source(here("pipelines", "transform", "src", "wranglers", "demographics", "wrangle_2021_age_sex.R"))

demographics_aggregation <- function() {
  input_dir <- here("pipelines", "ingest", "input", "metadata")
  metadata_layer <- "population"
  fname <- "population_mtl_by_district.csv"

  df_2016 <- wrangle_2016_age_sex(input_dir, metadata_layer, fname)
  df_2021 <- wrangle_2021_age_sex(input_dir, metadata_layer, fname)

  df <- bind_rows(df_2016, df_2021)

  output_dir <- here("pipelines", "transform", "input")
  dir_create(output_dir, recurse = TRUE)
  output_path <- file.path(output_dir, "census_population.parquet")
  write_parquet(df, output_path)

  message(sprintf("Wrote %d rows to %s", nrow(df), output_path))
  output_path
}
