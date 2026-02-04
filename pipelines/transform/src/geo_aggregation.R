# Geo aggregation - write geo layers as GeoJSON
library(dplyr)
library(here)
library(fs)

output_dir <- here("pipelines", "transform", "input")
dir_create(output_dir, recurse = TRUE)

geo_aggregation <- function() {

  # Load and simplify districts with rmapshaper (better topology preservation)
  districts <- sf::st_read(here("pipelines", "ingest", "input", "geo", "districts-electoraux-2021.geojson"), quiet = TRUE) |>
    rmapshaper::ms_simplify(keep = 0.05, keep_shapes = TRUE)

  # Load CMA boundary, filter to Montreal, transform to WGS84, simplify aggressively
  boundary <- sf::st_read(here("pipelines", "ingest", "input", "geo", "cma_boundary_file_census", "lcma000b21a_e.shp"), quiet = TRUE) |>
    filter(DGUID == "2021S0503462") |>
    sf::st_transform(crs = 4326) |>
    rmapshaper::ms_simplify(keep = 0.05, keep_shapes = TRUE)

  # Write as GeoJSON files (winding order fixed on frontend with @turf/rewind)
  districts_path <- here("pipelines", "transform", "input", "districts.geojson")
  boundary_path <- here("pipelines", "transform", "input", "boundary.geojson")

  sf::st_write(districts, districts_path, delete_dsn = TRUE, quiet = TRUE)
  sf::st_write(boundary, boundary_path, delete_dsn = TRUE, quiet = TRUE)

  message(sprintf("Wrote %d districts to %s", nrow(districts), districts_path))
  message(sprintf("Wrote %d boundary features to %s", nrow(boundary), boundary_path))

  list(districts = districts_path, boundary = boundary_path)
}
