# Geo aggregation - combine geo layers into TopoJSON

geo_aggregation <- function() {
  library(sf)
  library(dplyr)
  library(here)
  library(fs)
  library(geojsonio)

  transform_dir <- here("pipelines", "transform", "input")
  dir_create(transform_dir, recurse = TRUE)

  # Load and simplify districts
  districts <- st_read(here("pipelines", "ingest", "input", "geo", "districts-electoraux-2021.geojson"), quiet = TRUE) |>
    st_simplify(dTolerance = 0.0001, preserveTopology = TRUE) |>
    mutate(layer = "districts")

  # Load CMA boundary, filter to Montreal, transform to WGS84, simplify
  boundary <- st_read(here("pipelines", "ingest", "input", "geo", "cma_boundary_file_census", "lcma000b21a_e.shp"), quiet = TRUE) |>
    filter(DGUID == "2021S0503462") |>
    st_transform(crs = 4326) |>
    st_simplify(dTolerance = 0.0001, preserveTopology = TRUE) |>
    mutate(layer = "boundary")

  # Combine and write TopoJSON
  combined <- bind_rows(
    districts |> select(geometry, layer, everything()),
    boundary |> select(geometry, layer, everything())
  )

  output_path <- here("pipelines", "transform", "input", "montreal.topojson")
  topojson_write(combined, file = output_path)

  message(sprintf("Wrote %d districts + %d boundary features to %s",
                  nrow(districts), nrow(boundary), output_path))
  output_path
}
