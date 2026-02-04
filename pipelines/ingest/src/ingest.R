# Ingest functions - download Montreal datasets
# Sourced by etl.R, not a maestro pipeline itself

library(httr2)
library(readxl)
library(readr)
library(here)
library(fs)

download_file <- function(url, output_path, filename, sheet_name = NULL, force = FALSE) {
  dir_create(output_path, recurse = TRUE)
  filepath <- file.path(output_path, filename)
  is_zip <- tools::file_ext(filename) == "zip"

  # For zips, check if extract directory exists; for others, check the file
  check_path <- if (is_zip) {
    file.path(output_path, tools::file_path_sans_ext(filename))
  } else {
    filepath
  }

  if ((file_exists(check_path) || dir_exists(check_path)) && !force) {
    message(sprintf("Skipping %s (already exists)", filename))
    return(check_path)
  }

  message(sprintf("Downloading %s: %s", filename, url))

  if (!is.null(sheet_name)) {
    temp_file <- tempfile(fileext = paste0(".", tools::file_ext(url)))
    request(url) |>
      req_headers(`User-Agent` = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)") |>
      req_perform(path = temp_file)
    df <- read_excel(temp_file, sheet = sheet_name)
    write_csv(df, filepath)
    file_delete(temp_file)
    return(filepath)
  }

  req <- request(url) |>
    req_headers(`User-Agent` = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

  if (is_zip) {
    temp_file <- tempfile(fileext = ".zip")
    req_perform(req, path = temp_file)
    extract_dir <- file.path(output_path, tools::file_path_sans_ext(filename))
    dir_create(extract_dir, recurse = TRUE)
    unzip(temp_file, exdir = extract_dir)
    message("Extracted: ", paste(unzip(temp_file, list = TRUE)$Name, collapse = ", "))
    file_delete(temp_file)
    return(extract_dir)
  } else {
    req_perform(req, path = filepath)
    return(filepath)
  }
}

# Individual ingest functions
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

# Population metadata sources
POPULATION_SOURCES <- list(
  list(
    year = "2021",
    url = "https://ville.montreal.qc.ca/pls/portal/docs/PAGE/MTL_STATS_FR/MEDIA/DOCUMENTS/DONN%C9ES%20DU%20RECENSEMENT%20DE%202021_AGGLOM%C9RATION%20DE%20MONTR%C9AL_TOTAUX%20ET%20POURCENTAGES_0.XLSX",
    sheet_name = "Recensement2021_Totaux et %",
    filename = "population_mtl_by_district.csv"
  ),
  list(
    year = "2016",
    url = "http://ville.montreal.qc.ca/pls/portal/url/ITEM/55637C4923B8B03EE0530A930132B03E",
    sheet_name = "01_Population, Densité",
    filename = "population_mtl_by_district.csv"
  ),
  list(
    year = "2011",
    url = "http://ville.montreal.qc.ca/pls/portal/url/ITEM/55637C4923B8B03EE0530A930132B03E",
    sheet_name = "01_Population, Densité",
    filename = "population_mtl_by_district.csv"
  ),
  list(
    year = "2006",
    url = "https://donnees.montreal.ca/dataset/ca61c35d-9963-4f62-b963-073aa284e3bd/resource/d809ba17-a8f8-4405-8b79-a70815a25085/download/annuaire-statistique-2006-agglo-mtlenstats.xls",
    sheet_name = "01_Population_SR",
    filename = "population_mtl_by_district.csv"
  ),
  list(
    year = "2001",
    url = "http://ville.montreal.qc.ca/pls/portal/url/ITEM/085DF59B4B35D07CE0430A930132D07C",
    sheet_name = NULL,
    filename = "population_mtl_by_district.pdf"
  )
)

population_by_district <- function(force = FALSE) {
  paths <- c()
  for (src in POPULATION_SOURCES) {
    path <- download_file(
      url = src$url,
      output_path = here("pipelines", "ingest", "input", "metadata", "population", src$year),
      filename = src$filename,
      sheet_name = src$sheet_name,
      force = force
    )
    paths <- c(paths, path)
  }
  return(paths)
}