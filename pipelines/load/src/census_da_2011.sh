#!/bin/bash
# Build 2011 DA-level census star schema from StatCan Census Profile bulk CSV + DA boundary shapefile.
# Writes intermediate year-specific files; census_da_combine.sh merges all years.
#
# NOTE: 2011 Census Profile at DA level includes demographics and language only.
#       Income, tenure, immigration were in the NHS (not available at DA level).
#
# Format differences from 2021:
#   - String Characteristic names (not numeric CHARACTERISTIC_ID)
#   - Columns: Geo_Code, Total, Male, Female (not ALT_GEO_CODE, C1_COUNT_TOTAL, ...)
#   - 18 age groups (top bucket: "85 years and over" vs 2021's four 85+ sub-buckets)
#   - Topic column for disambiguation
#
# Reads:
#   - ingest/input/census_da/2011/quebec_da_2011/*QUE*.csv
#   - ingest/input/geo/da_boundary_2011/*.shp
# Writes:
#   - load/output/geo_da_2011.parquet
#   - load/output/census_demographics_2011.parquet
#   - load/output/census_economics_2011.parquet

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INGEST_DIR="$(cd "$SCRIPT_DIR/../../ingest/input" && pwd)"
OUTPUT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/output"
mkdir -p "$OUTPUT_DIR"

# Locate input files
DA_CSV=$(find "$INGEST_DIR/census_da/2011/quebec_da_2011" -name '*QUE*.csv' | head -1)
DA_SHP=$(find "$INGEST_DIR/geo/da_boundary_2011" -name '*.shp' | head -1)

if [ -z "$DA_CSV" ]; then
  echo "Error: 2011 DA bulk CSV not found in $INGEST_DIR/census_da/2011/quebec_da_2011/"
  exit 1
fi
if [ -z "$DA_SHP" ]; then
  echo "Error: 2011 DA boundary shapefile not found in $INGEST_DIR/geo/da_boundary_2011/"
  exit 1
fi

echo "Building 2011 star schema from Census Profile CSV..."
echo "  CSV: $DA_CSV"
echo "  SHP: $DA_SHP"

duckdb -c "
INSTALL spatial; LOAD spatial;

-- ================================================================
-- 1. geo_da_2011.parquet — geometry layer (one row per DA)
--    2011 shapefile has DAUID but no LANDAREA; get area from CSV.
-- ================================================================
COPY (
  SELECT
    2011 AS year,
    b.DAUID AS geo_uid,
    b.DAUID AS da_name,
    a.area_sqkm,
    b.geom
  FROM ST_Read('$DA_SHP') b
  JOIN (
    SELECT Geo_Code, TRY_CAST(Total AS DOUBLE) AS area_sqkm
    FROM read_csv('$DA_CSV', all_varchar=true, encoding='latin-1')
    WHERE Geo_Code LIKE '2466%'
      AND TRIM(Characteristic) = 'Land area (square km)'
  ) a ON b.DAUID = a.Geo_Code
  WHERE b.DAUID LIKE '2466%'
    AND b.DAUID IN (
      SELECT Geo_Code
      FROM read_csv('$DA_CSV', all_varchar=true, encoding='latin-1')
      WHERE Geo_Code LIKE '2466%'
        AND TRIM(Characteristic) = 'Population in 2011'
        AND TRY_CAST(Total AS DOUBLE) > 0
    )
) TO '$OUTPUT_DIR/geo_da_2011.parquet' (FORMAT PARQUET);

-- ================================================================
-- 2. census_demographics_2011.parquet — age pyramid (long format)
--    18 five-year age groups × 3 sexes per DA.
--    Top bucket is '85+' (2021 has 85-89, 90-94, 95-99, 100+).
-- ================================================================
COPY (
  WITH age_data AS (
    SELECT
      Geo_Code AS geo_uid,
      TRIM(Characteristic) AS char_name,
      TRY_CAST(Total AS DOUBLE) AS total,
      TRY_CAST(Male AS DOUBLE) AS male,
      TRY_CAST(Female AS DOUBLE) AS female
    FROM read_csv('$DA_CSV', all_varchar=true, encoding='latin-1')
    WHERE Geo_Code LIKE '2466%'
      AND Topic = 'Age characteristics'
      AND TRIM(Characteristic) IN (
        '0 to 4 years', '5 to 9 years', '10 to 14 years',
        '15 to 19 years', '20 to 24 years', '25 to 29 years',
        '30 to 34 years', '35 to 39 years', '40 to 44 years',
        '45 to 49 years', '50 to 54 years', '55 to 59 years',
        '60 to 64 years', '65 to 69 years', '70 to 74 years',
        '75 to 79 years', '80 to 84 years', '85 years and over'
      )
  ),
  labeled AS (
    SELECT geo_uid,
      CASE char_name
        WHEN '0 to 4 years'       THEN '0-4'
        WHEN '5 to 9 years'       THEN '5-9'
        WHEN '10 to 14 years'     THEN '10-14'
        WHEN '15 to 19 years'     THEN '15-19'
        WHEN '20 to 24 years'     THEN '20-24'
        WHEN '25 to 29 years'     THEN '25-29'
        WHEN '30 to 34 years'     THEN '30-34'
        WHEN '35 to 39 years'     THEN '35-39'
        WHEN '40 to 44 years'     THEN '40-44'
        WHEN '45 to 49 years'     THEN '45-49'
        WHEN '50 to 54 years'     THEN '50-54'
        WHEN '55 to 59 years'     THEN '55-59'
        WHEN '60 to 64 years'     THEN '60-64'
        WHEN '65 to 69 years'     THEN '65-69'
        WHEN '70 to 74 years'     THEN '70-74'
        WHEN '75 to 79 years'     THEN '75-79'
        WHEN '80 to 84 years'     THEN '80-84'
        WHEN '85 years and over'  THEN '85+'
      END AS age_group,
      total, male, female
    FROM age_data
  )
  SELECT 2011 AS year, geo_uid, age_group, 'total' AS sex, total AS population FROM labeled
  UNION ALL
  SELECT 2011, geo_uid, age_group, 'male' AS sex, male AS population FROM labeled
  UNION ALL
  SELECT 2011, geo_uid, age_group, 'female' AS sex, female AS population FROM labeled
) TO '$OUTPUT_DIR/census_demographics_2011.parquet' (FORMAT PARQUET);

-- ================================================================
-- 3. census_economics_2011.parquet — generic measures (long format)
--    Limited to Census Profile variables available at DA:
--    pop_total, dwellings_total, median_age, lang_mother_english, lang_mother_french
--    NO income, tenure, or immigration (those were in NHS, not at DA).
-- ================================================================
COPY (
  WITH pop_dwell AS (
    SELECT
      Geo_Code AS geo_uid,
      TRIM(Characteristic) AS char_name,
      TRY_CAST(Total AS DOUBLE) AS c_total,
      TRY_CAST(Male AS DOUBLE) AS c_male,
      TRY_CAST(Female AS DOUBLE) AS c_female
    FROM read_csv('$DA_CSV', all_varchar=true, encoding='latin-1')
    WHERE Geo_Code LIKE '2466%'
      AND Topic = 'Population and dwelling counts'
      AND TRIM(Characteristic) IN ('Population in 2011', 'Private dwellings occupied by usual residents')
  ),
  age_stats AS (
    SELECT
      Geo_Code AS geo_uid,
      TRIM(Characteristic) AS char_name,
      TRY_CAST(Total AS DOUBLE) AS c_total,
      TRY_CAST(Male AS DOUBLE) AS c_male,
      TRY_CAST(Female AS DOUBLE) AS c_female
    FROM read_csv('$DA_CSV', all_varchar=true, encoding='latin-1')
    WHERE Geo_Code LIKE '2466%'
      AND Topic = 'Age characteristics'
      AND TRIM(Characteristic) = 'Median age of the population'
  ),
  lang AS (
    SELECT
      Geo_Code AS geo_uid,
      TRIM(Characteristic) AS char_name,
      TRY_CAST(Total AS DOUBLE) AS c_total,
      TRY_CAST(Male AS DOUBLE) AS c_male,
      TRY_CAST(Female AS DOUBLE) AS c_female
    FROM read_csv('$DA_CSV', all_varchar=true, encoding='latin-1')
    WHERE Geo_Code LIKE '2466%'
      AND Topic = 'Detailed mother tongue'
      AND TRIM(Characteristic) IN ('English', 'French')
  )
  -- population
  SELECT 2011 AS year, geo_uid, 'pop_total' AS variable, 'total' AS sex, c_total AS value
  FROM pop_dwell WHERE char_name = 'Population in 2011'
  UNION ALL
  -- dwellings
  SELECT 2011, geo_uid, 'dwellings_total', 'total', c_total
  FROM pop_dwell WHERE char_name = 'Private dwellings occupied by usual residents'
  UNION ALL
  -- median age (total, male, female)
  SELECT 2011, geo_uid, 'median_age', 'total', c_total FROM age_stats
  UNION ALL
  SELECT 2011, geo_uid, 'median_age', 'male', c_male FROM age_stats
  UNION ALL
  SELECT 2011, geo_uid, 'median_age', 'female', c_female FROM age_stats
  UNION ALL
  -- language (mother tongue)
  SELECT 2011, geo_uid, 'lang_mother_english', 'total', c_total
  FROM lang WHERE char_name = 'English'
  UNION ALL
  SELECT 2011, geo_uid, 'lang_mother_french', 'total', c_total
  FROM lang WHERE char_name = 'French'
) TO '$OUTPUT_DIR/census_economics_2011.parquet' (FORMAT PARQUET);
"

echo "Done (2011):"
echo "  $OUTPUT_DIR/geo_da_2011.parquet"
echo "  $OUTPUT_DIR/census_demographics_2011.parquet"
echo "  $OUTPUT_DIR/census_economics_2011.parquet"
