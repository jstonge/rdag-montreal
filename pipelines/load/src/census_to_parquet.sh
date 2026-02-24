#!/bin/bash
# Convert census GeoJSON to spatial Parquet (WKB geometry) via DuckDB
# Reads from transform/input, writes to load/output

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INPUT_DIR="$(cd "$SCRIPT_DIR/../../transform/input" && pwd)"
OUTPUT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/output"
mkdir -p "$OUTPUT_DIR"

convert_census() {
  local level="$1"
  local input="$INPUT_DIR/census_${level}.geojson"
  local output="$OUTPUT_DIR/census_${level}.parquet"

  if [ ! -f "$input" ]; then
    echo "Skipping census_${level}: $input not found"
    return
  fi

  echo "Converting census_${level}.geojson → census_${level}.parquet..."
  duckdb -c "
INSTALL spatial; LOAD spatial;

COPY (
    SELECT
        geo_uid,
        name                          AS da_name,
        region_name,
        population,
        households,
        dwellings,
        area_sqkm,

        -- age
        avg_age_sex_total,
        pop_total,
        pop_age_0to14,
        pop_age_15to64,
        pop_age_65plus,

        -- income (median)
        median_income_household,
        median_income_aftertax_household,
        median_income_total,
        median_income_male,
        median_income_female,
        median_income_aftertax_total,
        median_income_aftertax_male,
        median_income_aftertax_female,

        -- income (average)
        avg_income_household,
        avg_income_aftertax_household,
        avg_total_income_total,
        avg_total_income_male,
        avg_total_income_female,

        -- housing tenure
        dwellings_total,
        tenure_owner,
        tenure_renter,

        -- language
        lang_mother_english,
        lang_mother_french,

        -- immigration
        pop_immigrant,

        -- admin codes
        csd_uid,
        cd_uid,
        ct_uid,
        cma_uid,

        geom
    FROM ST_Read('$input')
    WHERE population > 0
) TO '$output' (FORMAT PARQUET);
"
  echo "Done: $output"
}

convert_census "da"
convert_census "ct"
