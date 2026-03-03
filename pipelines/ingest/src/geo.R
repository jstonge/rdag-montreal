# Geo ingest - boundary and hydrography downloads

source(here::here("pipelines", "ingest", "src", "download_utils.R"))

districts_electoraux_2021 <- function(force = FALSE) {
  download_file(
    url = "https://donnees.montreal.ca/dataset/70acec75-c2b4-4d26-a399-facc7b0ad9bf/resource/d0c1467b-a551-42df-98b4-057e00a84275/download/districts-electoraux-2021.geojson",
    output_path = here("pipelines", "ingest", "input", "geo"),
    filename = "districts-electoraux-2021.geojson",
    force = force
  )
}

montreal_cma <- function(force = FALSE) {
  download_file(
    url = "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lcma000b21a_e.zip",
    output_path = here("pipelines", "ingest", "input", "geo"),
    filename = "cma_boundary_file_census.zip",
    force = force
  )
}

boundary_file_census_2021 <- function(force = FALSE) {
  download_file(
    url = "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lcsd000b21a_e.zip",
    output_path = here("pipelines", "ingest", "input", "geo"),
    filename = "boundary_file_census.zip",
    force = force
  )
}

hydrographie_2020 <- function(force = FALSE) {
  download_file(
    url = "https://donnees.montreal.ca/dataset/ead1ac6f-f37c-4326-a9b9-4508d94bbc45/resource/73d4571c-fd7a-465a-aa19-05c3b24222cc/download/hydrographie-2020.zip",
    output_path = here("pipelines", "ingest", "input", "geo"),
    filename = "hydrographie_2020.zip",
    force = force
  )
}

montreal_boundary <- function(force = FALSE) {
  download_file(
    url = "https://donnees.montreal.ca/fr/dataset/b628f1da-9dc3-4bb1-9875-1470f891afb1/resource/92cb062a-11be-4222-9ea5-867e7e64c5ff/download/limites-terrestres.geojson",
    output_path = here("pipelines", "ingest", "input", "geo"),
    filename = "limites-terrestres.geojson",
    force = force
  )
}

montreal_adm_boundaries <- function(force = FALSE) {
  download_file(
    url = "https://donnees.montreal.ca/dataset/9797a946-9da8-41ec-8815-f6b276dec7e9/resource/6b313375-d9bc-4dc3-af8e-ceae3762ae6e/download/limites-administratives-agglomeration-nad83.geojson",
    output_path = here("pipelines", "ingest", "input", "geo"),
    filename = "limites-administratives-agglomeration-nad83.geojson",
    force = force
  )
}

da_boundary_2021 <- function(force = FALSE) {
  download_file(
    url = "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lda_000b21a_e.zip",
    output_path = here("pipelines", "ingest", "input", "geo"),
    filename = "da_boundary_2021.zip",
    force = force
  )
}
