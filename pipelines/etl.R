# Montreal ETL Pipeline
# This is the maestro pipeline

source(here::here("pipelines", "ingest", "src", "ingest.R"))
source(here::here("pipelines", "transform", "src", "geo_aggregation.R"))
source(here::here("pipelines", "transform", "src", "metadata_aggregation.R"))

# =============================================================================
# INGEST PIPELINES
# =============================================================================

#' Ingest electoral districts
#' @maestroFrequency 1 day
#' @maestroStartTime 2026-01-29
#' @maestroOutputs transform_geo
ingest_districts <- function() {
  districts_electoraux_2021()
}

#' Ingest Montreal CMA boundary
#' @maestroFrequency 1 day
#' @maestroStartTime 2026-01-29
#' @maestroOutputs transform_geo
ingest_cma <- function() {
  montreal_cma()
}

#' Ingest population data
#' @maestroFrequency 1 day
#' @maestroStartTime 2026-01-29
#' @maestroOutputs transform_metadata
ingest_population <- function() {
  # Population data is downloaded via Excel sheets in ingest.R
  # For now this is a placeholder - add population_by_district() when ready
  message("Population data ready")
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
