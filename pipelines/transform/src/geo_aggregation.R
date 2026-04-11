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

  # Load and simplify streets from Overpass export
  streets_input <- here("pipelines", "ingest", "input", "geo", "montreal-streets.geojson")
  if (file.exists(streets_input)) {
    # Clip to Montreal bounding box (Overpass sometimes returns features outside the area)
    montreal_bbox <- sf::st_bbox(c(xmin = -73.97, ymin = 45.40, xmax = -73.47, ymax = 45.70),
                                  crs = sf::st_crs(4326)) |> sf::st_as_sfc()
    streets <- sf::st_read(streets_input, quiet = TRUE) |>
      sf::st_intersection(montreal_bbox) |>
      rmapshaper::ms_simplify(keep = 0.1, keep_shapes = TRUE)

    streets_path <- here("pipelines", "transform", "input", "streets.geojson")
    sf::st_write(streets, streets_path, delete_dsn = TRUE, quiet = TRUE)
    message(sprintf("Wrote %d street features to %s", nrow(streets), streets_path))
  } else {
    message("Skipping streets (run montreal_streets_osm() in ingest first)")
    streets_path <- NULL
  }

  # Extract contour lines from DEM for Mount Royal (requires gdal_contour on PATH)
  dem_file <- here("pipelines", "ingest", "input", "geo", "mt_royal_dem.tif")
  contours_path <- here("pipelines", "transform", "input", "contours.geojson")
  if (file.exists(dem_file)) {
    system2("gdal_contour", c("-a", "elevation", "-i", "10", dem_file, contours_path, "-f", "GeoJSON"))
    message(sprintf("Wrote contours to %s", contours_path))
  } else {
    message("Skipping contours (run montreal_dem() in ingest first)")
    contours_path <- NULL
  }

  list(districts = districts_path, boundary = boundary_path, streets = streets_path, contours = contours_path)
}
