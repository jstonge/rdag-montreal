# Tests for geo_aggregation output
# Replicates frontend logic to verify TopoJSON structure

library(testthat)
library(jsonlite)
library(here)

test_that("TopoJSON matches reference structure", {
  # Load local file
  local_path <- here("pipelines", "transform", "input", "montreal.topojson")
  local <- fromJSON(local_path)

  # Load reference file
  ref_url <- "https://raw.githubusercontent.com/jstonge/dag-montreal/refs/heads/main/src/dag_montreal/defs/transform/input/montreal.topojson"
  ref <- fromJSON(ref_url)

  # Check top-level structure
  expect_equal(local$type, "Topology")
  expect_true("objects" %in% names(local))
  expect_true("data" %in% names(local$objects))
  expect_true("arcs" %in% names(local))

  # Check objects.data structure matches reference
  expect_equal(names(local$objects), names(ref$objects))
  expect_equal(local$objects$data$type, ref$objects$data$type)
})

test_that("Features can be filtered by layer like frontend does", {
  local_path <- here("pipelines", "transform", "input", "montreal.topojson")
  local <- fromJSON(local_path)

  geometries <- local$objects$data$geometries
  props <- geometries$properties

  # Frontend does: features.filter(f => f.properties.layer === 'districts')
  districts <- props[props$layer == "districts", ]
  boundary <- props[props$layer == "boundary", ]

  expect_equal(nrow(districts), 59, info = "Should have 59 districts")
  expect_equal(nrow(boundary), 1, info = "Should have 1 boundary")
})

test_that("District properties match reference", {
  local_path <- here("pipelines", "transform", "input", "montreal.topojson")
  local <- fromJSON(local_path)

  ref_url <- "https://raw.githubusercontent.com/jstonge/dag-montreal/refs/heads/main/src/dag_montreal/defs/transform/input/montreal.topojson"
  ref <- fromJSON(ref_url)

  local_props <- local$objects$data$geometries$properties
  ref_props <- ref$objects$data$geometries$properties

  # Check required properties exist
  required_props <- c("layer", "nom", "arrondissement")
  for (prop in required_props) {
    expect_true(prop %in% names(local_props),
                info = sprintf("Missing property: %s", prop))
  }

  # Check district names match
  local_districts <- local_props[local_props$layer == "districts", ]
  ref_districts <- ref_props[ref_props$layer == "districts", ]

  local_names <- sort(local_districts$nom)
  ref_names <- sort(ref_districts$nom)

  expect_equal(local_names, ref_names,
               info = "District names should match reference")

  # Check arrondissement names match
  local_arr <- sort(unique(local_districts$arrondissement))
  ref_arr <- sort(unique(ref_districts$arrondissement))

  expect_equal(local_arr, ref_arr,
               info = "Arrondissement names should match reference")
})

test_that("Geometry types are valid", {
  local_path <- here("pipelines", "transform", "input", "montreal.topojson")
  local <- fromJSON(local_path)

  geometries <- local$objects$data$geometries

  # All geometry types should be Polygon or MultiPolygon
  valid_types <- c("Polygon", "MultiPolygon")
  expect_true(all(geometries$type %in% valid_types),
              info = "All geometries should be Polygon or MultiPolygon")
})

test_that("Arcs are properly encoded", {
  local_path <- here("pipelines", "transform", "input", "montreal.topojson")
  local <- fromJSON(local_path)

  # Should have arcs array

  expect_true(is.list(local$arcs))
  expect_gt(length(local$arcs), 0, info = "Should have arcs")

  # Should have transform for quantized coordinates
  expect_true("transform" %in% names(local),
              info = "Should have transform for quantization")
  expect_true(all(c("scale", "translate") %in% names(local$transform)))
})

test_that("Bounding box is reasonable for Montreal", {
  local_path <- here("pipelines", "transform", "input", "montreal.topojson")
  local <- fromJSON(local_path)

  bbox <- local$bbox

  # Montreal approximate bounds: lon [-74, -73], lat [45.4, 45.7]
  expect_true(bbox[1] > -75 && bbox[1] < -73, info = "Min longitude in range")
  expect_true(bbox[2] > 45 && bbox[2] < 46, info = "Min latitude in range")
  expect_true(bbox[3] > -74 && bbox[3] < -73, info = "Max longitude in range")
  expect_true(bbox[4] > 45 && bbox[4] < 46, info = "Max latitude in range")
})

# Run all tests
test_file(here("pipelines", "transform", "tests", "test_geo_aggregation.R"))
