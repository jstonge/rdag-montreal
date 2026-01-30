# Geo aggregation - write geo layers as GeoParquet
library(sf)
library(dplyr)
library(here)
library(fs)
library(rmapshaper)
library(geoarrow)
library(arrow)

transform_dir <- here("pipelines", "transform", "input")
dir_create(transform_dir, recurse = TRUE)

geo_aggregation <- function() {

  # Load and simplify districts with rmapshaper (better topology preservation)
  districts <- st_read(here("pipelines", "ingest", "input", "geo", "districts-electoraux-2021.geojson"), quiet = TRUE) |>
    ms_simplify(keep = 0.15, keep_shapes = TRUE)

  # Load CMA boundary, filter to Montreal, transform to WGS84, simplify
  boundary <- st_read(here("pipelines", "ingest", "input", "geo", "cma_boundary_file_census", "lcma000b21a_e.shp"), quiet = TRUE) |>
    filter(DGUID == "2021S0503462") |>
    st_transform(crs = 4326) |>
    ms_simplify(keep = 0.15, keep_shapes = TRUE)

  # Write as separate GeoParquet files
  districts_path <- here("pipelines", "transform", "input", "districts.parquet")
  boundary_path <- here("pipelines", "transform", "input", "boundary.parquet")

  districts |>
    tibble::as_tibble() |>
    write_parquet(districts_path)

  boundary |>
    tibble::as_tibble() |>
    write_parquet(boundary_path)

  message(sprintf("Wrote %d districts to %s", nrow(districts), districts_path))
  message(sprintf("Wrote %d boundary features to %s", nrow(boundary), boundary_path))

  list(districts = districts_path, boundary = boundary_path)
}
