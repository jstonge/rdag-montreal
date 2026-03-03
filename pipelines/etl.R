# Montreal ETL Pipeline
# This is the maestro pipeline

source(here::here("pipelines", "ingest", "src", "geo.R"))
source(here::here("pipelines", "ingest", "src", "census.R"))
source(here::here("pipelines", "transform", "src", "geo_aggregation.R"))
source(here::here("pipelines", "transform", "src", "metadata_aggregation.R"))
source(here::here("pipelines", "transform", "src", "census_aggregation.R"))

# =============================================================================
# INGEST PIPELINES
# =============================================================================

#' Ingest electoral districts
#' @maestroOutputs transform_geo
ingest_districts <- function() {
  districts_electoraux_2021()
}

#' Ingest Montreal CMA boundary
#' @maestroOutputs transform_geo
ingest_cma <- function() {
  montreal_cma()
}

#' Ingest population data
#' @maestroOutputs transform_metadata
ingest_population <- function() {
  population_by_district()
  census_1991_by_district()
  census_1996_by_district()
  census_2001_by_district()
  census_2006_by_district()
  census_2011_by_district()
  census_2016_by_district()
  census_2021_by_district()
}

# =============================================================================
# TRANSFORM PIPELINES
# =============================================================================

#' Transform geo layers into TopoJSON
#' @maestroInputs ingest_districts ingest_cma
transform_geo <- function(.input) {
  geo_aggregation()
}

#' Transform population metadata
#' @maestroInputs ingest_population
transform_metadata <- function(.input) {
  metadata_aggregation()
}

#' Ingest DA-level census bulk CSV + boundary shapefile
#' @maestroOutputs load_census_da
ingest_census_da <- function() {
  census_2021_da()
  da_boundary_2021()
}

# =============================================================================
# LOAD PIPELINES
# =============================================================================

#' Build DA census parquet from bulk CSV + shapefile via DuckDB
#' @maestroInputs ingest_census_da
load_census_da <- function(.input) {
  system2("bash", here::here("pipelines", "load", "src", "census_da_from_bulk.sh"))
}
