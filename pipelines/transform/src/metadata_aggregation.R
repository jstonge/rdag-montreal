# Metadata aggregation - combine population data across census years
library(dplyr)
library(readr)
library(here)
library(fs)

output_dir <- here("pipelines", "transform", "input")
dir_create(output_dir, recurse = TRUE)

source(here("pipelines", "transform", "src", "wranglers", "population", "wrangle_1991.R"))
source(here("pipelines", "transform", "src", "wranglers", "population", "wrangle_1996.R"))
source(here("pipelines", "transform", "src", "wranglers", "population", "wrangle_2001.R"))
source(here("pipelines", "transform", "src", "wranglers", "population", "wrangle_2006.R"))
source(here("pipelines", "transform", "src", "wranglers", "population", "wrangle_2011.R"))
source(here("pipelines", "transform", "src", "wranglers", "population", "wrangle_2016.R"))
source(here("pipelines", "transform", "src", "wranglers", "population", "wrangle_2021.R"))

metadata_aggregation <- function() {

  # Population layer
  input_dir <- here("pipelines", "ingest", "input", "metadata")
  metadata_layer <- "population"
  fname <- "population_mtl_by_district.csv"

  df_1991 <- wrangle_1991(input_dir, metadata_layer) |> mutate(source = "StatCan 1991 Census profiles")
  df_1996 <- wrangle_1996(input_dir, metadata_layer) |> mutate(source = "StatCan 1996 Census profiles")
  df_2001 <- wrangle_2001(input_dir, metadata_layer) |> mutate(source = "StatCan 2001 Census profiles")
  df_2006 <- wrangle_2006(input_dir, metadata_layer, fname)
  df_2011 <- wrangle_2011(input_dir, metadata_layer, fname)
  df_2016 <- wrangle_2016(input_dir, metadata_layer, fname)
  df_2021 <- wrangle_2021(input_dir, metadata_layer, fname)

  df <- bind_rows(df_1991, df_1996, df_2001, df_2006, df_2011, df_2016, df_2021)

  # OUTPUT
  output_path <- here("pipelines", "transform", "input", "metadata.csv")
  write_csv(df, output_path)

  message(sprintf("Wrote %d rows to %s", nrow(df), output_path))
  output_path
}
