# rdag-montreal

Census and geographic data pipeline for the Montreal agglomeration (1991-2021).
R-based ETL feeding a JavaScript frontend that uses DuckDB-WASM for on-the-fly aggregation.

## Project structure

```
pipelines/
  etl.R                          # Maestro orchestrator (ingest -> transform -> load)
  ingest/src/
    census.R                     # StatCan + Ville de Montréal downloads
    geo.R                        # Boundary downloads (districts, CMA, DA shapefile)
    download_utils.R             # Shared download/caching helpers
  ingest/input/
    metadata/population/
      1991/ .. 1996/             # Per-CSD CSV files from StatCan (one per municipality)
      2001_statcan/              # Per-CSD CSVs (post-demerger)
      2006/ 2011/ 2016/ 2021/    # Ville de Montréal population_mtl_by_district.csv
      2006_statcan/ 2011_statcan/  # Per-CSD CSVs from StatCan
      2016_statcan/ 2021_statcan/  # Bulk Quebec CSV extracts (montreal_population_YYYY.csv
                                   #   + montreal_census_YYYY.csv for full profiles)
    census_da/
      2011/                      # DA-level bulk CSV from StatCan Census Profile (quebec_da_2011/)
      2021/                      # DA-level bulk CSV from StatCan (quebec_da_2021/)
    geo/                         # Boundary shapefiles and GeoJSON
      da_boundary_2001/          # 2001 DA boundary (converted from .e00 to .gpkg)
      da_boundary_2006/          # 2006 DA boundary shapefile from StatCan
      da_boundary_2011/          # 2011 DA boundary shapefile from StatCan
      da_boundary_2021/          # 2021 DA boundary shapefile from StatCan
  transform/src/
    metadata_aggregation.R       # Combines population wranglers -> metadata.csv
    demographics_aggregation.R   # Combines age×sex wranglers -> census_population.parquet
    census_aggregation.R         # [LEGACY] cancensus API pull (replaced by bulk CSV pipeline)
    geo_aggregation.R            # Geo layer processing
    wranglers/
      utils.R                    # normalize_name()
      population/                # Per-year population wranglers (wrangle_YYYY.R)
      demographics/              # Per-year age×sex wranglers (wrangle_YYYY_age_sex.R)
  transform/input/               # Transform outputs (consumed by load or frontend)
    metadata.csv                 # (arrondissement, population, year, source)
    census_population.parquet    # (arrondissement, year, age_group, sex, population, source)
  transform/tests/
    test_metadata.R              # Cross-validates population across sources (7 tests, 14 expectations)
  load/src/
    census_da_2021.sh            # DuckDB: 2021 bulk CSV + shapefile -> year-specific parquet
    census_da_2011.sh            # DuckDB: 2011 Census Profile CSV + shapefile -> year-specific parquet
    census_da_combine.sh         # DuckDB: combine year-specific files into final multi-year parquet
  load/output/
    geo_da.parquet               # Geometry layer (2011 + 2021, ~6.4K DAs with geom)
    census_demographics.parquet  # Age × sex fact table (2011 + 2021, ~377K rows)
    census_economics.parquet     # Generic measures fact table (2011 + 2021, ~117K rows)
    census_income.parquet        # Income distribution fact table (2021 only, ~373K rows)
```

## Key design decisions

### Hybrid StatCan + Ville de Montréal sourcing (2006-2021)

StatCan publishes per-CSD census profiles. Each demerged municipality (Westmount,
Dorval, etc.) is its own CSD, so StatCan covers them directly. But the 19
arrondissements of Ville de Montréal are sub-divisions of a single CSD (2466023),
so StatCan only provides the city-wide total. For arrondissement-level data we
use Ville de Montréal's census profile files.

Each year from 2006 onward has three functions:
- `wrangle_YYYY_statcan()` - demerged municipalities from StatCan
- `wrangle_YYYY_mtl()` - all districts from Ville de Montréal
- `wrangle_YYYY()` - combines: StatCan rows + Ville de Montréal rows not in StatCan

Per-row `source` column tracks provenance. Cross-validated in test_metadata.R.

### ETL layer boundaries

- **Ingest**: download and cache raw files. No transformation. Output = raw CSVs/PDFs/XLS.
- **Transform**: clean, normalize, merge, cross-validate. Output = canonical datasets
  (source-agnostic, reusable). `metadata.csv`, `census_population.parquet`.
- **Load**: reshape for a specific consumer. Column selection, geo joins, format conversion.
  Output = `load/output/` files ready for the frontend.

### DA-level pipeline (multi-year star schema)

The primary geographic unit for analysis. Replaces the earlier cancensus API approach.
Uses a star schema: one geometry table joined with long-format fact tables.
All tables have a `year` column for multi-census comparison.

**Pipeline**: `ingest_census_da` (download bulk CSVs + DA shapefiles for 2011 + 2021) →
`load_census_da` (DuckDB: `census_da_2021.sh` → `census_da_2011.sh` → `census_da_combine.sh`)

**2011 vs 2021 data availability at DA level:**
- 2011 Census Profile: demographics (18 age groups, top = 85+), language, dwellings
- 2011 has NO income, tenure, or immigration (those were in NHS, not available at DA)
- 2021 Census Profile: demographics (21 age groups), income, tenure, immigration, language
- 2011: 3,194 DAs | 2021: 3,219 DAs (DA boundaries change between censuses)

**Output tables** (Montreal agglomeration, CD 2466):

```
geo_da.parquet                 ← geometry layer (one row per DA per year)
├── year, geo_uid, da_name, area_sqkm, geom
└── ~6.4K rows (3,194 for 2011 + 3,219 for 2021)

census_demographics.parquet    ← age × sex fact table (long format)
├── year, geo_uid, age_group, sex, population
└── ~377K rows (2011: 18 age groups × 3 sexes + 2021: 21 age groups × 3 sexes)

census_economics.parquet       ← generic measures fact table (long format)
├── year, geo_uid, variable, sex, value
└── ~117K rows (2011: 5 variables + 2021: 19 variables with LIM-AT/LICO-AT)

census_income.parquet          ← income distribution fact table (long format)
├── year, geo_uid, income_type, bracket, sex, count
└── ~373K rows (2021 only — 4 income types × leaf brackets × DAs)
```

**Frontend query patterns** (DuckDB-WASM):
```sql
-- Choropleth: color map by median household income (2021)
SELECT g.geom, e.value FROM geo_da g
JOIN census_economics e ON g.geo_uid = e.geo_uid AND g.year = e.year
WHERE e.variable = 'median_income_household' AND e.year = 2021

-- Age pyramid for one DA (single year)
SELECT age_group, sex, population FROM census_demographics
WHERE geo_uid = '24660837' AND year = 2021 AND sex != 'total'

-- Compare population across years
SELECT g.year, g.geo_uid, g.geom, e.value AS pop
FROM geo_da g
JOIN census_economics e ON g.geo_uid = e.geo_uid AND g.year = e.year
WHERE e.variable = 'pop_total'

-- Income histogram (2021 only)
SELECT bracket, count FROM census_income
WHERE geo_uid = '24660101' AND income_type = 'total_income' AND sex = 'total'
```

**2021 CHARACTERISTIC_ID mapping — demographics** (21 five-year age groups, C1/C2/C3):
```
10 → 0-4    14 → 15-19   20 → 45-49   26 → 70-74   31 → 90-94
11 → 5-9    15 → 20-24   21 → 50-54   27 → 75-79   32 → 95-99
12 → 10-14  16 → 25-29   22 → 55-59   28 → 80-84   33 → 100+
             17 → 30-34   23 → 60-64   30 → 85-89
             18 → 35-39   25 → 65-69
             19 → 40-44
```

**2011 demographics** — 18 age groups matched by string Characteristic name (e.g., "0 to 4 years").
Top bucket "85 years and over" → "85+" (2021 has four sub-buckets: 85-89, 90-94, 95-99, 100+).

**2021 CHARACTERISTIC_ID mapping — economics** (19 variables):
```
1    pop_total               252  avg_income_household
4    dwellings_total         253  avg_income_aftertax_household
39   avg_age                 318  median_income (total/male/female via C1/C2/C3)
243  median_income_household 319  median_income_aftertax (total/male/female)
244  median_income_aftertax  333  avg_income (total/male/female)
     _household              340  lim_at_count (total/male/female)
396  lang_mother_english     345  lim_at_prevalence (total, %)
397  lang_mother_french      355  lico_at_count (total/male/female)
1415 tenure_owner            360  lico_at_prevalence (total, %)
1416 tenure_renter
1529 pop_immigrant
```

**2011 economics** — 5 variables only (Census Profile, no NHS):
pop_total, dwellings_total, median_age (not avg_age), lang_mother_english, lang_mother_french.

**2021 CHARACTERISTIC_ID mapping — income** (4 distribution types, leaf brackets only):
```
Person total income:      156,158-167,169,170  (13 brackets, C1/C2/C3)
Person after-tax income:  172,174-183,185,186  (13 brackets, C1/C2/C3)
Household total income:   261-275,277-280      (19 brackets, C1 only)
Household after-tax:      282-296,298-300      (18 brackets, C1 only)
```

### census_population.parquet (fact table)

Long format: one row per (arrondissement, year, age_group, sex). This is the atomic
unit for DuckDB-WASM on the frontend:
- Age pyramid: `WHERE arrondissement = X AND sex != 'total' GROUP BY age_group, sex`
- Total pop: `WHERE sex = 'total' GROUP BY arrondissement`
- Model inputs: cells are observations for `y ~ age + sex + (1|arrondissement)`

### StatCan bulk CSV extraction (CSD level, 2016 & 2021)

The bulk Quebec CSD downloads are 300-600MB ZIPs. `extract_mtl_census()` in census.R
uses the geo starting row index to read only the 16 Montreal-area CSDs:
- `n_chars = 1`: population-only extract (backward compat with population wranglers)
- `n_chars = NA`: full profile (~2,631 characteristics per CSD, used by demographics wranglers)

### Montreal CSD codes

16 CSDs in the agglomeration (consistent 2011-2021):
```
2466007  2466023  2466032  2466047  2466058  2466062  2466072  2466087
2466092  2466097  2466102  2466107  2466112  2466117  2466127  2466142
```
CSD 2466023 = Montréal proper (arrondissements). Rest = demerged municipalities.
All DAs in CD 2466 (`ALT_GEO_CODE LIKE '2466%'`) belong to the Montreal agglomeration.

## Conventions

- R scripts use `here::here()` for all paths
- Arrondissement names normalized via `normalize_name()` (en-dash -> hyphen, curly quote -> straight)
- Tests use `testthat`. Run: `Rscript -e 'library(here); testthat::test_file(here("pipelines/transform/tests/test_metadata.R"))'`
- StatCan bulk CSVs: encoding varies by year. 2016 has UTF-8 BOM, 2021 is Latin-1.
  `extract_mtl_census()` auto-detects via BOM check. DuckDB scripts use `encoding='latin-1'`.
- The `renv` lockfile manages R dependencies
- DuckDB scripts (`load/src/*.sh`) use `INSTALL spatial; LOAD spatial;` for geo operations

## What's next

- CT-level pipeline (same bulk CSV approach as DA, different GEONO)
- Geo hierarchy lookup table (DA -> CT -> arrondissement) for multi-level aggregation
- Pre-computed model results table for the inferential layer on the frontend
- Frontend integration with star schema (geo_da + census_demographics + census_economics + census_income)
