# Metadata aggregation - combine population data across census years
library(dplyr)
library(readr)
library(here)
library(fs)

output_dir <- here("pipelines", "transform", "input")
dir_create(output_dir, recurse = TRUE)


normalize_name <- function(name) {
  name |>
    stringr::str_replace_all("–", "-") |>
    stringr::str_replace_all("'", "'")
}

wrangle_2011_2016 <- function(input_dir, metadata_layer, year, fname) {
  file_path <- here(input_dir, metadata_layer, year, fname)
  # Column name pattern: "Population en 2011" or similar
  pop_col <- paste0("Population en ", year)

  df <- read_csv(file_path, skip = 2, show_col_types = FALSE) |>
    slice(1:(n() - 4)) |>
    rename(arrondissement = 1) |>
    select(arrondissement, population = any_of(pop_col)) |>
    filter(!arrondissement %in% c("Autres villes", "Ville de Montréal", "AGGLOMÉRATION DE MONTRÉAL")) |>
    mutate(
      arrondissement = normalize_name(arrondissement),
      year = as.integer(year),
      population = parse_number(as.character(population))
    )

  df
}

metadata_aggregation <- function() {

  # Population layer
  input_dir <- here("pipelines", "ingest", "input", "metadata")
  metadata_layer <- "population"
  fname <- "population_mtl_by_district.csv"

  df_2011 <- wrangle_2011_2016(input_dir, metadata_layer, "2011", fname)
  df_2016 <- wrangle_2011_2016(input_dir, metadata_layer, "2016", fname)

  df <- bind_rows(df_2011, df_2016)

  # OUTPUT
  output_path <- here("pipelines", "transform", "input", "metadata.csv")
  write_csv(df, output_path)

  message(sprintf("Wrote %d rows to %s", nrow(df), output_path))
  output_path
}