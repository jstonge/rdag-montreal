# Geo aggregation - combine geo layers into TopoJSON
library(sf)
library(dplyr)
library(here)
library(fs)
library(geojsonio)
library(rmapshaper)
library(jsonlite)

transform_dir <- here("pipelines", "transform", "input")
dir_create(transform_dir, recurse = TRUE)

geo_aggregation <- function() {


  # Load and simplify districts with rmapshaper (better topology preservation)
  districts <- st_read(here("pipelines", "ingest", "input", "geo", "districts-electoraux-2021.geojson"), quiet = TRUE) |>
    ms_simplify(keep = 0.15, keep_shapes = TRUE) |>
    mutate(layer = "districts")

  # Load CMA boundary, filter to Montreal, transform to WGS84, simplify
  boundary <- st_read(here("pipelines", "ingest", "input", "geo", "cma_boundary_file_census", "lcma000b21a_e.shp"), quiet = TRUE) |>
    filter(DGUID == "2021S0503462") |>
    st_transform(crs = 4326) |>
    ms_simplify(keep = 0.15, keep_shapes = TRUE) |>
    mutate(layer = "boundary")

  # Combine and write TopoJSON
  combined <- bind_rows(
    districts |> select(geometry, layer, everything()),
    boundary |> select(geometry, layer, everything())
  )

  output_path <- here("pipelines", "transform", "input", "montreal.topojson")
  topojson_write(combined, file = output_path, object_name = "data", quantization = 1e4)

  # Post-process: fix "NA" strings and add id at geometry level (required by D3/TopoJSON)
  topo <- jsonlite::fromJSON(output_path, simplifyVector = FALSE)
  for (i in seq_along(topo$objects$data$geometries)) {
    geom <- topo$objects$data$geometries[[i]]
    # Add id at geometry level from properties$id
    topo$objects$data$geometries[[i]]$id <- geom$properties$id
    # Convert "NA" strings to NULL in properties
    for (prop in names(geom$properties)) {
      val <- geom$properties[[prop]]
      if (identical(val, "NA")) {
        topo$objects$data$geometries[[i]]$properties[[prop]] <- NULL
      }
    }
  }
  jsonlite::write_json(topo, output_path, auto_unbox = TRUE, null = "null")

  message(sprintf("Wrote %d districts + %d boundary features to %s",
                  nrow(districts), nrow(boundary), output_path))
  output_path
}
