# Download utilities - shared helper for all ingest functions

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
