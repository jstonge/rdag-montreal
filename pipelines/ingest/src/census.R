# Census ingest - population and other census variable downloads
# Organized by what we download (census data), not by source (StatCan vs Ville de Montréal).
# Sources vary across years but the data concern is stable.

source(here::here("pipelines", "ingest", "src", "download_utils.R"))

# =============================================================================
# Population metadata sources (2001-2021, Ville de Montréal)
# =============================================================================
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
    sheet_name = "01_Population, Densit\u00e9",
    filename = "population_mtl_by_district.csv"
  ),
  list(
    year = "2011",
    url = "http://ville.montreal.qc.ca/pls/portal/url/ITEM/55637C4923B8B03EE0530A930132B03E",
    sheet_name = "01_Population, Densit\u00e9",
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

# =============================================================================
# 1991 Census: one CSV per census subdivision (CSD) in CD 2466.
# GIDs scraped from the StatCan geo index page.
# Each CSV has "Population, 1991 (2)" on row 4.
# =============================================================================
CENSUS_1991_BASE_URL <- "https://www12.statcan.gc.ca/english/census91/data/profiles/File.cfm?S=0&LANG=E&A=R&PID=29&GID=%s&D1=0&D2=0&D3=0&D4=0&D5=0&D6=0&OFT=CSV"
CENSUS_1991_INDEX_URL <- "https://www12.statcan.gc.ca/english/census91/data/profiles/Geo-index-eng.cfm?TABID=5&LANG=E&APATH=3&DETAIL=1&DIM=0&FL=A&FREE=1&GC=0&GID=17221&GK=0&GRP=1&PID=29&PRID=0&PTYPE=3&S=0&SHOWALL=No&SUB=0&Temporal=1991&THEME=113&VID=0&VNAMEE=&VNAMEF=&D1=0&D2=0&D3=0&D4=0&D5=0&D6=0"

CENSUS_1991 <- list(
  list(gid = "17220", name = "Montr\u00e9al-Est"),
  list(gid = "17221", name = "Anjou"),
  list(gid = "17222", name = "Saint-L\u00e9onard"),
  list(gid = "17223", name = "Montr\u00e9al-Nord"),
  list(gid = "17224", name = "Montr\u00e9al"),
  list(gid = "17225", name = "Westmount"),
  list(gid = "17226", name = "Verdun"),
  list(gid = "17227", name = "LaSalle"),
  list(gid = "17228", name = "Montr\u00e9al-Ouest"),
  list(gid = "17229", name = "Saint-Pierre"),
  list(gid = "17230", name = "C\u00f4te-Saint-Luc"),
  list(gid = "17231", name = "Hampstead"),
  list(gid = "17232", name = "Outremont"),
  list(gid = "17233", name = "Mont-Royal"),
  list(gid = "17234", name = "Saint-Laurent"),
  list(gid = "17235", name = "Lachine"),
  list(gid = "17236", name = "Dorval"),
  list(gid = "17238", name = "Pointe-Claire"),
  list(gid = "17239", name = "Kirkland"),
  list(gid = "17240", name = "Beaconsfield"),
  list(gid = "17241", name = "Baie-d'Urf\u00e9"),
  list(gid = "17242", name = "Sainte-Anne-de-Bellevue"),
  list(gid = "17243", name = "Senneville"),
  list(gid = "17244", name = "Pierrefonds"),
  list(gid = "17245", name = "Sainte-Genevi\u00e8ve"),
  list(gid = "17246", name = "Dollard-des-Ormeaux"),
  list(gid = "17247", name = "Roxboro"),
  list(gid = "17248", name = "Saint-Rapha\u00ebl-de-l'\u00cele-Bizard")
)

census_1991_by_district <- function(force = FALSE) {
  output_dir <- here("pipelines", "ingest", "input", "metadata", "population", "1991")
  dir_create(output_dir, recurse = TRUE)

  paths <- c()
  for (src in CENSUS_1991) {
    filename <- paste0(src$name, ".csv")
    url <- sprintf(CENSUS_1991_BASE_URL, src$gid)
    path <- download_file(
      url = url,
      output_path = output_dir,
      filename = filename,
      force = force
    )
    paths <- c(paths, path)
  }
  return(paths)
}

# =============================================================================
# 1996 Census: same pattern as 1991, different PID (35782) and GIDs.
# Population row: "Population, 1996 (100% data) [3]"
# =============================================================================
CENSUS_1996_BASE_URL <- "https://www12.statcan.gc.ca/english/census96/data/profiles/File.cfm?S=0&LANG=E&A=R&PID=35782&GID=%s&D1=0&D2=0&D3=0&D4=0&D5=0&D6=0&OFT=CSV"
CENSUS_1996_INDEX_URL <- "https://www12.statcan.gc.ca/english/census96/data/profiles/Geo-index-eng.cfm?TABID=5&LANG=E&APATH=3&DETAIL=0&DIM=0&FL=A&FREE=0&GC=0&GID=0&GK=0&GRP=1&PID=35782&PRID=0&PTYPE=3&S=0&SHOWALL=0&SUB=0&Temporal=1996&THEME=34&VID=0&VNAMEE=&VNAMEF=&D1=0&D2=0&D3=0&D4=0&D5=0&D6=0"

CENSUS_1996 <- list(
  list(gid = "201130", name = "Montr\u00e9al-Est"),
  list(gid = "201131", name = "Anjou"),
  list(gid = "201132", name = "Saint-L\u00e9onard"),
  list(gid = "201133", name = "Montr\u00e9al-Nord"),
  list(gid = "201134", name = "Montr\u00e9al"),
  list(gid = "201135", name = "Westmount"),
  list(gid = "201136", name = "Verdun"),
  list(gid = "201137", name = "LaSalle"),
  list(gid = "201138", name = "Montr\u00e9al-Ouest"),
  list(gid = "201139", name = "Saint-Pierre"),
  list(gid = "201140", name = "C\u00f4te-Saint-Luc"),
  list(gid = "201141", name = "Hampstead"),
  list(gid = "201142", name = "Outremont"),
  list(gid = "201143", name = "Mont-Royal"),
  list(gid = "201144", name = "Saint-Laurent"),
  list(gid = "201145", name = "Lachine"),
  list(gid = "201146", name = "Dorval"),
  list(gid = "201148", name = "Pointe-Claire"),
  list(gid = "201149", name = "Kirkland"),
  list(gid = "201150", name = "Beaconsfield"),
  list(gid = "201151", name = "Baie-d'Urf\u00e9"),
  list(gid = "201152", name = "Sainte-Anne-de-Bellevue"),
  list(gid = "201153", name = "Senneville"),
  list(gid = "201154", name = "Pierrefonds"),
  list(gid = "201155", name = "Sainte-Genevi\u00e8ve"),
  list(gid = "201156", name = "Dollard-des-Ormeaux"),
  list(gid = "201157", name = "Roxboro"),
  list(gid = "201158", name = "L'\u00cele-Bizard")
)

census_1996_by_district <- function(force = FALSE) {
  output_dir <- here("pipelines", "ingest", "input", "metadata", "population", "1996")
  dir_create(output_dir, recurse = TRUE)

  paths <- c()
  for (src in CENSUS_1996) {
    filename <- paste0(src$name, ".csv")
    url <- sprintf(CENSUS_1996_BASE_URL, src$gid)
    path <- download_file(
      url = url,
      output_path = output_dir,
      filename = filename,
      force = force
    )
    paths <- c(paths, path)
  }
  return(paths)
}

# =============================================================================
# 2001 Census: per-CSD profiles from StatCan (pre-demerger boundaries).
# Row 3 = "Population, 2001 - 100% Data [1]"
# Cross-validates against the PDF-derived 2001 data from Ville de Montréal.
# =============================================================================
CENSUS_2001_BASE_URL <- "https://www12.statcan.gc.ca/english/census01/products/standard/profiles/File.cfm?S=0&LANG=E&A=R&PID=72849&GID=%s&D1=0&D2=0&D3=0&D4=0&D5=0&D6=0&OFT=CSV"
CENSUS_2001_INDEX_URL <- "https://www12.statcan.gc.ca/english/census01/products/standard/profiles/Geo-index-eng.cfm?TABID=5&LANG=E&APATH=1&DETAIL=0&DIM=0&FL=A&FREE=0&GC=0&GID=0&GK=0&GRP=1&PID=72849&PRID=0&PTYPE=56079&S=0&SHOWALL=0&SUB=0&Temporal=2001&THEME=57&VID=0&VNAMEE=&VNAMEF=&D1=0&D2=0&D3=0&D4=0&D5=0&D6=0"

CENSUS_2001 <- list(
  list(gid = "443762", name = "Montr\u00e9al-Est"),
  list(gid = "443770", name = "Anjou"),
  list(gid = "443838", name = "Saint-L\u00e9onard"),
  list(gid = "443971", name = "Montr\u00e9al-Nord"),
  list(gid = "444120", name = "Montr\u00e9al"),
  list(gid = "445984", name = "Westmount"),
  list(gid = "446021", name = "Verdun"),
  list(gid = "446131", name = "LaSalle"),
  list(gid = "446270", name = "Montr\u00e9al-Ouest"),
  list(gid = "446281", name = "C\u00f4te-Saint-Luc"),
  list(gid = "446334", name = "Lachine"),
  list(gid = "446404", name = "Hampstead"),
  list(gid = "446418", name = "Outremont"),
  list(gid = "446462", name = "Mont-Royal"),
  list(gid = "446499", name = "Saint-Laurent"),
  list(gid = "446627", name = "Dorval"),
  list(gid = "446664", name = "Pointe-Claire"),
  list(gid = "446716", name = "Kirkland"),
  list(gid = "446750", name = "Beaconsfield"),
  list(gid = "446788", name = "Baie-d'Urf\u00e9"),
  list(gid = "446796", name = "Sainte-Anne-de-Bellevue"),
  list(gid = "446805", name = "Senneville"),
  list(gid = "446808", name = "Pierrefonds"),
  list(gid = "446905", name = "Sainte-Genevi\u00e8ve"),
  list(gid = "446912", name = "Dollard-des-Ormeaux"),
  list(gid = "446992", name = "Roxboro"),
  list(gid = "447004", name = "L'\u00cele-Bizard")
)

census_2001_by_district <- function(force = FALSE) {
  output_dir <- here("pipelines", "ingest", "input", "metadata", "population", "2001_statcan")
  dir_create(output_dir, recurse = TRUE)

  paths <- c()
  for (src in CENSUS_2001) {
    filename <- paste0(src$name, ".csv")
    url <- sprintf(CENSUS_2001_BASE_URL, src$gid)
    path <- download_file(
      url = url,
      output_path = output_dir,
      filename = filename,
      force = force
    )
    paths <- c(paths, path)
  }
  return(paths)
}

# =============================================================================
# 2006 Census: per-CSD profiles from StatCan (post-demerger boundaries).
# Same CSV format as 1991/1996 but different URL pattern.
# Row 3 = "Population, 2001", Row 4 = "Population, 2006"
# Useful for cross-validating against Ville de Montréal 2006 data.
# =============================================================================
CENSUS_2006_BASE_URL <- "https://www12.statcan.gc.ca/census-recensement/2006/dp-pd/prof/rel/File.cfm?S=0&LANG=E&A=R&PID=94533&GID=%s&D1=0&D2=0&D3=0&D4=0&D5=0&D6=0&OFT=CSV"
CENSUS_2006_INDEX_URL <- "https://www12.statcan.gc.ca/census-recensement/2006/dp-pd/prof/rel/Geo-index-eng.cfm?TABID=5&LANG=E&APATH=3&DETAIL=0&DIM=0&FL=A&FREE=0&GC=0&GID=0&GK=0&GRP=0&PID=94533&PRID=0&PTYPE=89103&S=0&SHOWALL=0&SUB=0&Temporal=2006&THEME=81&VID=0&VNAMEE=&VNAMEF=&D1=0&D2=0&D3=0&D4=0&D5=0&D6=0"

CENSUS_2006 <- list(
  list(gid = "773016", name = "Montr\u00e9al (CD)"),
  list(gid = "773017", name = "Montr\u00e9al-Est"),
  list(gid = "773018", name = "Montr\u00e9al"),
  list(gid = "773019", name = "Westmount"),
  list(gid = "773020", name = "Montr\u00e9al-Ouest"),
  list(gid = "773021", name = "C\u00f4te-Saint-Luc"),
  list(gid = "773022", name = "Hampstead"),
  list(gid = "773023", name = "Mont-Royal"),
  list(gid = "773024", name = "Dorval"),
  list(gid = "773026", name = "Pointe-Claire"),
  list(gid = "773027", name = "Kirkland"),
  list(gid = "773028", name = "Beaconsfield"),
  list(gid = "773029", name = "Baie-D'Urf\u00e9"),
  list(gid = "773030", name = "Sainte-Anne-de-Bellevue"),
  list(gid = "773031", name = "Senneville"),
  list(gid = "773032", name = "Dollard-Des Ormeaux")
)

census_2006_by_district <- function(force = FALSE) {
  output_dir <- here("pipelines", "ingest", "input", "metadata", "population", "2006_statcan")
  dir_create(output_dir, recurse = TRUE)

  paths <- c()
  for (src in CENSUS_2006) {
    filename <- paste0(src$name, ".csv")
    url <- sprintf(CENSUS_2006_BASE_URL, src$gid)
    path <- download_file(
      url = url,
      output_path = output_dir,
      filename = filename,
      force = force
    )
    paths <- c(paths, path)
  }
  return(paths)
}

# =============================================================================
# 2011 Census: per-CSD profiles from StatCan (post-demerger boundaries).
# Different URL pattern from older censuses: uses CSD codes, not GIDs.
# CSV format is also different: proper columnar CSV with Topic/Characteristics headers.
# Row 3 = "Population in 2011", row 4 = "Population in 2006" (col 4 = Total).
# =============================================================================
CENSUS_2011_BASE_URL <- "https://www12.statcan.gc.ca/census-recensement/2011/dp-pd/prof/details/download-telecharger/CSV.cfm?Lang=E&Geo1=CSD&Code1=%s&Geo2=PR&Code2=01&Data=Count&SearchText=&SearchType=Begins&SearchPR=01&B1=All&Custom=&TABID=1"
CENSUS_2011_INDEX_URL <- "https://www12.statcan.gc.ca/census-recensement/2011/dp-pd/prof/search-recherche/lst/page.cfm?Lang=E&TABID=1&G=1&Geo1=PR&Code1=01&Geo2=PR&Code2=01&GEOCODE=24"

CENSUS_2011 <- list(
  list(code = "2466007", name = "Montr\u00e9al-Est"),
  list(code = "2466023", name = "Montr\u00e9al"),
  list(code = "2466032", name = "Westmount"),
  list(code = "2466047", name = "Montr\u00e9al-Ouest"),
  list(code = "2466058", name = "C\u00f4te-Saint-Luc"),
  list(code = "2466062", name = "Hampstead"),
  list(code = "2466072", name = "Mont-Royal"),
  list(code = "2466087", name = "Dorval"),
  list(code = "2466092", name = "L'\u00cele-Dorval"),
  list(code = "2466097", name = "Pointe-Claire"),
  list(code = "2466102", name = "Kirkland"),
  list(code = "2466107", name = "Beaconsfield"),
  list(code = "2466112", name = "Baie-D'Urf\u00e9"),
  list(code = "2466117", name = "Sainte-Anne-de-Bellevue"),
  list(code = "2466127", name = "Senneville"),
  list(code = "2466142", name = "Dollard-Des Ormeaux")
)

census_2011_by_district <- function(force = FALSE) {
  output_dir <- here("pipelines", "ingest", "input", "metadata", "population", "2011_statcan")
  dir_create(output_dir, recurse = TRUE)

  paths <- c()
  for (src in CENSUS_2011) {
    filename <- paste0(src$name, ".csv")
    url <- sprintf(CENSUS_2011_BASE_URL, src$code)
    path <- download_file(
      url = url,
      output_path = output_dir,
      filename = filename,
      force = force
    )
    paths <- c(paths, path)
  }
  return(paths)
}

# =============================================================================
# 2016 Census: comprehensive CSV for all Quebec CSDs (ZIP download).
# We download, extract, and filter to Montreal CD 2466 population rows.
# =============================================================================
CENSUS_2016_URL <- "https://www12.statcan.gc.ca/census-recensement/2016/dp-pd/prof/details/download-telecharger/comp/GetFile.cfm?Lang=E&FILETYPE=CSV&GEONO=065"

# Montreal demerged CSD codes (same across 2011-2021)
MONTREAL_CSD_CODES <- c("2466007", "2466023", "2466032", "2466047", "2466058",
                         "2466062", "2466072", "2466087", "2466092", "2466097",
                         "2466102", "2466107", "2466112", "2466117", "2466127",
                         "2466142")

# Helper: extract Montreal CSD rows from a bulk Quebec CSD CSV.
# Uses the geo starting row index to read only the relevant rows.
# n_chars: number of characteristics to read per CSD (default 1 = population only).
#          Use NA to read all characteristics for each CSD.
extract_mtl_census <- function(data_csv, geo_csv, output_path, n_chars = 1L) {
  geo <- readr::read_csv(geo_csv, show_col_types = FALSE)
  # Normalize column names (2016 vs 2021 have slightly different names)
  names(geo) <- c("geo_code", "geo_name", "line_number")
  # Match by CSD code: geo_code may be numeric (2466032) or DGUID (2021A00052466032)
  mtl_geo <- geo[grepl(paste(MONTREAL_CSD_CODES, collapse = "|"), as.character(geo$geo_code)), ]

  # Figure out how many rows per CSD from the geo index spacing
  if (is.na(n_chars)) {
    if (nrow(mtl_geo) >= 2) {
      # Use gap between first two Montreal CSDs
      n_chars <- mtl_geo$line_number[2] - mtl_geo$line_number[1]
    } else {
      # Use gap to next CSD in the full index
      idx <- which(geo$geo_code == mtl_geo$geo_code[1])
      if (idx < nrow(geo)) {
        n_chars <- geo$line_number[idx + 1] - geo$line_number[idx]
      } else {
        stop("Cannot determine characteristics count from geo index")
      }
    }
  }

  # Detect encoding: 2016 CSV has UTF-8 BOM, 2021 is Latin-1.
  bom <- readBin(data_csv, "raw", n = 3)
  encoding <- if (identical(bom, as.raw(c(0xef, 0xbb, 0xbf)))) "UTF-8" else "latin1"
  loc <- readr::locale(encoding = encoding)
  header <- readr::read_csv(data_csv, show_col_types = FALSE, n_max = 0, locale = loc)
  col_names <- names(header)

  # When reading many characteristics, force all columns to character to avoid
  # type conflicts across CSDs (e.g. "..." suppression markers vs numbers).
  col_types <- if (n_chars > 1) readr::cols(.default = "c") else NULL

  rows <- lapply(seq_len(nrow(mtl_geo)), function(i) {
    readr::read_csv(data_csv, show_col_types = FALSE,
                    skip = mtl_geo$line_number[i] - 1, n_max = n_chars,
                    col_names = col_names, locale = loc, col_types = col_types)
  })

  df <- dplyr::bind_rows(rows)
  readr::write_csv(df, output_path)
  message(sprintf("Extracted %d rows (%d CSDs x %d chars) to %s",
                  nrow(df), nrow(mtl_geo), n_chars, output_path))
  output_path
}

census_2016_by_district <- function(force = FALSE) {
  output_dir <- here("pipelines", "ingest", "input", "metadata", "population", "2016_statcan")
  dir_create(output_dir, recurse = TRUE)

  data_csv <- file.path(output_dir, "98-401-X2016065_English_CSV_data.csv")
  geo_csv  <- file.path(output_dir, "Geo_starting_row_CSV.csv")

  # Download and extract ZIP if needed
  if (!file_exists(data_csv)) {
    zip_path <- file.path(output_dir, "quebec_csd_2016.zip")
    download_file(url = CENSUS_2016_URL, output_path = output_dir,
                  filename = "quebec_csd_2016.zip", force = force)
    unzip(zip_path, exdir = output_dir)
    file_delete(zip_path)
  }

  paths <- c()

  # Population-only extract (1 row per CSD) - used by existing population wranglers
  pop_path <- file.path(output_dir, "montreal_population_2016.csv")
  if (!file_exists(pop_path) || force) {
    extract_mtl_census(data_csv, geo_csv, pop_path, n_chars = 1L)
  }
  paths <- c(paths, pop_path)

  # Full census profile extract (all characteristics) - used by demographics wranglers
  full_path <- file.path(output_dir, "montreal_census_2016.csv")
  if (!file_exists(full_path) || force) {
    extract_mtl_census(data_csv, geo_csv, full_path, n_chars = NA)
  }
  paths <- c(paths, full_path)

  # Ville de Montréal age groups sheet (arrondissement-level data not in StatCan)
  # The 2016 XLS has age and sex in separate sheets (not cross-tabulated).
  # Sheet "02_Groupes d'âge, âge moyen" has total population by age group per district.
  mtl_dir <- here("pipelines", "ingest", "input", "metadata", "population", "2016")
  age_path <- file.path(mtl_dir, "age_mtl_by_district.csv")
  if (!file_exists(age_path) || force) {
    download_file(
      url = "http://ville.montreal.qc.ca/pls/portal/url/ITEM/55637C4923B8B03EE0530A930132B03E",
      output_path = mtl_dir,
      filename = "age_mtl_by_district.csv",
      sheet_name = "02_Groupes d'\u00e2ge, \u00e2ge moyen",
      force = TRUE
    )
  }
  paths <- c(paths, age_path)

  paths
}

# =============================================================================
# 2021 Census: same bulk download approach as 2016.
# =============================================================================
CENSUS_2021_URL <- "https://www12.statcan.gc.ca/census-recensement/2021/dp-pd/prof/details/download-telecharger/comp/GetFile.cfm?Lang=E&FILETYPE=CSV&GEONO=020"

census_2021_by_district <- function(force = FALSE) {
  output_dir <- here("pipelines", "ingest", "input", "metadata", "population", "2021_statcan")
  dir_create(output_dir, recurse = TRUE)

  data_csv <- file.path(output_dir, "98-401-X2021020_English_CSV_data.csv")
  geo_csv  <- file.path(output_dir, "98-401-X2021020_Geo_starting_row.CSV")

  # Download and extract ZIP if needed
  if (!file_exists(data_csv)) {
    zip_path <- file.path(output_dir, "quebec_csd_2021.zip")
    download_file(url = CENSUS_2021_URL, output_path = output_dir,
                  filename = "quebec_csd_2021.zip", force = force)
    unzip(zip_path, exdir = output_dir)
    file_delete(zip_path)
  }

  paths <- c()

  # Population-only extract (1 row per CSD) - used by existing population wranglers
  pop_path <- file.path(output_dir, "montreal_population_2021.csv")
  if (!file_exists(pop_path) || force) {
    extract_mtl_census(data_csv, geo_csv, pop_path, n_chars = 1L)
  }
  paths <- c(paths, pop_path)

  # Full census profile extract (all characteristics) - used by demographics wranglers
  full_path <- file.path(output_dir, "montreal_census_2021.csv")
  if (!file_exists(full_path) || force) {
    extract_mtl_census(data_csv, geo_csv, full_path, n_chars = NA)
  }
  paths <- c(paths, full_path)

  paths
}

# =============================================================================
# 2011 Census DA-level: bulk CSV for all Quebec DAs (Census Profile).
# ~156 MB ZIP. Different format from 2021 (string Characteristic names,
# different column names). DuckDB queries this directly.
# NOTE: Only Census Profile data (demographics, language, dwellings).
#       Income/tenure/immigration were in the NHS (not available at DA).
# =============================================================================
CENSUS_2011_DA_URL <- "https://www12.statcan.gc.ca/census-recensement/2011/dp-pd/prof/details/download-telecharger/comprehensive/comp_download.cfm?CTLG=98-316-XWE2011001&FMT=CSV1501"

census_2011_da <- function(force = FALSE) {
  output_dir <- here("pipelines", "ingest", "input", "census_da", "2011")
  dir_create(output_dir, recurse = TRUE)

  extract_dir <- file.path(output_dir, "quebec_da_2011")

  if (!dir_exists(extract_dir) || force) {
    download_file(
      url = CENSUS_2011_DA_URL,
      output_path = output_dir,
      filename = "quebec_da_2011.zip",
      force = force
    )
  }

  extract_dir
}

# =============================================================================
# 2021 Census DA-level: bulk CSV for all Quebec DAs (GEONO=006).
# ~528 MB ZIP. DuckDB queries this directly — no R extraction needed.
# =============================================================================
CENSUS_2021_DA_URL <- "https://www12.statcan.gc.ca/census-recensement/2021/dp-pd/prof/details/download-telecharger/comp/GetFile.cfm?Lang=E&FILETYPE=CSV&GEONO=006"

census_2021_da <- function(force = FALSE) {
  output_dir <- here("pipelines", "ingest", "input", "census_da", "2021")
  dir_create(output_dir, recurse = TRUE)

  # The ZIP extracts to a directory with the bulk CSV and geo index inside
  zip_path <- file.path(output_dir, "quebec_da_2021.zip")
  extract_dir <- file.path(output_dir, "quebec_da_2021")

  if (!dir_exists(extract_dir) || force) {
    download_file(
      url = CENSUS_2021_DA_URL,
      output_path = output_dir,
      filename = "quebec_da_2021.zip",
      force = force
    )
  }

  extract_dir
}
