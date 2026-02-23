# Census aggregation - pull demographic data from cancensus API
# Combines geometry + attributes for Montreal (2021 Census)
# Outputs both DA (Dissemination Area) and CT (Census Tract) levels
library(cancensus)
library(dplyr)
library(here)
library(fs)

# Census vectors to pull — confirm IDs with:
#   find_census_vectors("topic", dataset = "CA21", query_type = "semantic")
CENSUS_VECTORS <- c(
  # Population
  "v_CA21_1",    # Population, 2021
  # Age
  "v_CA21_8",    # 0-14 years
  "v_CA21_251",  # 15-64 years
  "v_CA21_254",  # 65+ years
  "v_CA21_386",  # Median age
  # Income
  "v_CA21_906",  # Median total income
  # Housing
  "v_CA21_4",    # Total private dwellings
  "v_CA21_4239", # Owner
  "v_CA21_4240", # Renter
  # Language & immigration
  "v_CA21_1144", # English mother tongue
  "v_CA21_1147", # French mother tongue
  "v_CA21_4404"  # Immigrants
)

fetch_census_level <- function(level) {
  get_census(
    dataset = "CA21",
    regions = list(CSD = "2466023"),
    vectors = CENSUS_VECTORS,
    level = level,
    geo_format = "sf"
  ) |>
    sf::st_transform(crs = 4326) |>
    rmapshaper::ms_simplify(keep = 0.05, keep_shapes = TRUE)
}

census_aggregation <- function() {
  output_dir <- here("pipelines", "transform", "input")
  dir_create(output_dir, recurse = TRUE)

  # Dissemination Areas
  census_da <- fetch_census_level("DA")
  da_path <- here("pipelines", "transform", "input", "census_da.geojson")
  sf::st_write(census_da, da_path, delete_dsn = TRUE, quiet = TRUE)
  message(sprintf("Wrote %d DAs to %s", nrow(census_da), da_path))

  # Census Tracts
  census_ct <- fetch_census_level("CT")
  ct_path <- here("pipelines", "transform", "input", "census_ct.geojson")
  sf::st_write(census_ct, ct_path, delete_dsn = TRUE, quiet = TRUE)
  message(sprintf("Wrote %d CTs to %s", nrow(census_ct), ct_path))

  list(da = da_path, ct = ct_path)
}
