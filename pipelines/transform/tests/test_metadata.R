# Cross-validation tests for the population metadata pipeline
#
# Strategy: each census file contains the *previous* census population.
# We compare those embedded values against our standalone wrangled data
# to catch parsing errors, name mismatches, or boundary issues.
# For 2006+ we also cross-validate StatCan vs Ville de Montréal for
# demerged municipalities.

library(dplyr)
library(readr)
library(here)
library(testthat)

source(here("pipelines", "transform", "src", "wranglers", "utils.R"))
source(here("pipelines", "transform", "src", "wranglers", "population", "wrangle_2001.R"))
source(here("pipelines", "transform", "src", "wranglers", "population", "wrangle_2006.R"))
source(here("pipelines", "transform", "src", "wranglers", "population", "wrangle_2011.R"))
source(here("pipelines", "transform", "src", "wranglers", "population", "wrangle_2016.R"))
source(here("pipelines", "transform", "src", "wranglers", "population", "wrangle_2021.R"))

input_dir <- here("pipelines", "ingest", "input", "metadata")
metadata_layer <- "population"
fname <- "population_mtl_by_district.csv"

# ---------------------------------------------------------------------------
# Helper: extract the *previous* census population from the 2006 file
# (col 5 = "Population en 2001", using post-demerger boundaries)
# ---------------------------------------------------------------------------
extract_2001_from_2006 <- function() {
  file_path <- here(input_dir, metadata_layer, "2006", fname)
  read_csv(file_path, skip = 2, show_col_types = FALSE) |>
    rename(arrondissement = 1) |>
    select(arrondissement, population = 5) |>
    filter(
      !is.na(arrondissement),
      !arrondissement %in% c("Autres villes", "Ville de Montréal",
                              "AGGLOMÉRATION DE MONTRÉAL",
                              "Source : Statistique Canada, Recensement de 2006")
    ) |>
    mutate(
      arrondissement = normalize_name(arrondissement),
      population = parse_number(as.character(population))
    )
}

# ---------------------------------------------------------------------------
# Helper: extract 2011 population from the 2016 file
# (col "Population en 2011")
# ---------------------------------------------------------------------------
extract_2011_from_2016 <- function() {
  file_path <- here(input_dir, metadata_layer, "2016", fname)
  read_csv(file_path, skip = 2, show_col_types = FALSE) |>
    slice(1:(n() - 4)) |>
    rename(arrondissement = 1) |>
    select(arrondissement, population = `Population en 2011`) |>
    filter(!arrondissement %in% c("Autres villes", "Ville de Montréal",
                                   "AGGLOMÉRATION DE MONTRÉAL")) |>
    mutate(
      arrondissement = normalize_name(arrondissement),
      population = parse_number(as.character(population))
    )
}

# ===========================================================================
# Test 1: 2016's "Population en 2011" matches our wrangled 2011 data
#   Same boundaries, same source (StatCan). Should match exactly.
# ===========================================================================
test_that("2011 population from 2016 file matches standalone 2011 wrangler", {
  from_2016 <- extract_2011_from_2016()
  standalone <- wrangle_2011(input_dir, metadata_layer, fname)

  joined <- inner_join(
    standalone |> select(arrondissement, pop_standalone = population),
    from_2016  |> select(arrondissement, pop_from_2016 = population),
    by = "arrondissement"
  )

  # All districts should join
  expect_equal(nrow(joined), nrow(standalone),
               label = "all 2011 districts matched by name")

  # Populations should be identical
  mismatches <- joined |> filter(pop_standalone != pop_from_2016)
  expect_equal(nrow(mismatches), 0,
               label = "no population mismatches between 2011 standalone and 2016 embedded")
})

# ===========================================================================
# Test 2: 2006's "Population en 2001" vs standalone 2001 data (StatCan)
#   Boundaries differ (2001 = pre-demerger). Districts that didn't change
#   should match; split/merged ones won't join. We test the ones that DO join.
# ===========================================================================
test_that("matching 2001 districts agree between 2006 retrospective and standalone 2001 (StatCan)", {
  from_2006  <- extract_2001_from_2006()
  standalone <- wrangle_2001(input_dir, metadata_layer)

  joined <- inner_join(
    standalone |> select(arrondissement, pop_standalone = population),
    from_2006  |> select(arrondissement, pop_from_2006 = population),
    by = "arrondissement"
  )

  # We expect some districts to match (those with stable boundaries)
  expect_gt(nrow(joined), 0, label = "at least some 2001 districts match by name")

  # StatCan 2001 and the 2006 retrospective should match exactly
  mismatches <- joined |> filter(pop_standalone != pop_from_2006)
  expect_equal(nrow(mismatches), 0,
               label = "no population mismatches for 2001")
})

# ===========================================================================
# Test 3: StatCan 2006 vs Ville de Montréal 2006 for demerged municipalities
#   Both sources are used in the combined wrangler. Cross-validate that they
#   agree for the demerged municipalities.
# ===========================================================================
test_that("StatCan 2006 and Ville de Montréal 2006 agree for demerged municipalities", {
  sc  <- wrangle_2006_statcan(input_dir, metadata_layer)
  mtl <- wrangle_2006_mtl(input_dir, metadata_layer, fname)

  joined <- inner_join(
    sc  |> select(arrondissement, pop_statcan = population),
    mtl |> select(arrondissement, pop_mtl = population),
    by = "arrondissement"
  )

  expect_gt(nrow(joined), 0, label = "at least some districts match by name")

  mismatches <- joined |> filter(pop_statcan != pop_mtl)
  if (nrow(mismatches) > 0) {
    message("Mismatches found:")
    print(mismatches)
  }
  expect_equal(nrow(mismatches), 0,
               label = "no population mismatches between StatCan and Ville de Montréal for 2006")
})

# ===========================================================================
# Test 4: StatCan 2011 vs Ville de Montréal 2011 for demerged municipalities
# ===========================================================================
test_that("StatCan 2011 and Ville de Montréal 2011 agree for demerged municipalities", {
  sc  <- wrangle_2011_statcan(input_dir, metadata_layer)
  mtl <- wrangle_2011_mtl(input_dir, metadata_layer, fname)

  joined <- inner_join(
    sc  |> select(arrondissement, pop_statcan = population),
    mtl |> select(arrondissement, pop_mtl = population),
    by = "arrondissement"
  )

  expect_gt(nrow(joined), 0, label = "at least some 2011 districts match by name")

  mismatches <- joined |> filter(pop_statcan != pop_mtl)
  if (nrow(mismatches) > 0) {
    message("Mismatches between StatCan and Ville de Montréal for 2011:")
    print(mismatches)
  }
  expect_equal(nrow(mismatches), 0,
               label = "no population mismatches between StatCan and Ville de Montréal for 2011")
})

# ===========================================================================
# Test 5: StatCan 2016 vs Ville de Montréal 2016 for demerged municipalities
# ===========================================================================
test_that("StatCan 2016 and Ville de Montréal 2016 agree for demerged municipalities", {
  sc  <- wrangle_2016_statcan(input_dir, metadata_layer)
  mtl <- wrangle_2016_mtl(input_dir, metadata_layer, fname)

  joined <- inner_join(
    sc  |> select(arrondissement, pop_statcan = population),
    mtl |> select(arrondissement, pop_mtl = population),
    by = "arrondissement"
  )

  expect_gt(nrow(joined), 0, label = "at least some 2016 districts match by name")

  mismatches <- joined |> filter(pop_statcan != pop_mtl)
  if (nrow(mismatches) > 0) {
    message("Mismatches between StatCan and Ville de Montréal for 2016:")
    print(mismatches)
  }
  expect_equal(nrow(mismatches), 0,
               label = "no population mismatches between StatCan and Ville de Montréal for 2016")
})

# ===========================================================================
# Test 6: StatCan 2021 vs Ville de Montréal 2021 for demerged municipalities
# ===========================================================================
test_that("StatCan 2021 and Ville de Montréal 2021 agree for demerged municipalities", {
  sc  <- wrangle_2021_statcan(input_dir, metadata_layer)
  mtl <- wrangle_2021_mtl(input_dir, metadata_layer, fname)

  joined <- inner_join(
    sc  |> select(arrondissement, pop_statcan = population),
    mtl |> select(arrondissement, pop_mtl = population),
    by = "arrondissement"
  )

  expect_gt(nrow(joined), 0, label = "at least some 2021 districts match by name")

  mismatches <- joined |> filter(pop_statcan != pop_mtl)
  if (nrow(mismatches) > 0) {
    message("Mismatches between StatCan and Ville de Montréal for 2021:")
    print(mismatches)
  }
  expect_equal(nrow(mismatches), 0,
               label = "no population mismatches between StatCan and Ville de Montréal for 2021")
})

# ===========================================================================
# Test 7: Basic sanity checks on the aggregated output
# ===========================================================================
test_that("aggregated metadata has expected years and no NA populations (except L'Île-Dorval)", {
  source(here("pipelines", "transform", "src", "metadata_aggregation.R"))
  metadata_aggregation()

  df <- read_csv(here("pipelines", "transform", "input", "metadata.csv"),
                 show_col_types = FALSE)

  expect_setequal(unique(df$year), c(1991, 1996, 2001, 2006, 2011, 2016, 2021))

  na_pops <- df |> filter(is.na(population))
  # L'Île-Dorval in 2006 has N/D -> NA, which is expected
  unexpected_na <- na_pops |>
    filter(!(arrondissement == "L'Île-Dorval" & year == 2006))
  expect_equal(nrow(unexpected_na), 0,
               label = "no unexpected NA populations")
})
