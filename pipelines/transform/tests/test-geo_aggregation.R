library(testthat)
library(sf)
library(here)

test_that("geo_aggregation produces valid TopoJSON with expected districts", {
  output_path <- here("pipelines", "transform", "input", "montreal.topojson")

  # Skip if output doesn't exist yet

  skip_if_not(file.exists(output_path), "TopoJSON output not found - run geo_aggregation first")

  # Read the TopoJSON
  gdf <- st_read(output_path, quiet = TRUE)

  # Check we have the expected number of districts
  districts <- gdf[gdf$layer == "districts", ]
  EXPECTED_NUMBER_DISTRICTS <- 59

  expect_equal(
    nrow(districts),
    EXPECTED_NUMBER_DISTRICTS,
    info = sprintf("Expected %d districts, got %d", EXPECTED_NUMBER_DISTRICTS, nrow(districts))
  )

  # Check we have the boundary layer
  boundary <- gdf[gdf$layer == "boundary", ]
  expect_gte(nrow(boundary), 1, info = "Expected at least 1 boundary feature")

  # Check geometries are valid

  expect_true(all(st_is_valid(gdf)), info = "All geometries should be valid")
})

test_that("geo_aggregation output has correct CRS", {
  output_path <- here("pipelines", "transform", "input", "montreal.topojson")

  skip_if_not(file.exists(output_path), "TopoJSON output not found - run geo_aggregation first")

  gdf <- st_read(output_path, quiet = TRUE)

  # Should be WGS84 (EPSG:4326)
  expect_equal(st_crs(gdf)$epsg, 4326L, info = "CRS should be WGS84 (EPSG:4326)")
})
