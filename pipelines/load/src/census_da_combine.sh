#!/bin/bash
# Combine year-specific parquet files into final multi-year star schema.
# Run after census_da_2011.sh and census_da_2021.sh.
#
# Reads:
#   - load/output/geo_da_2011.parquet, geo_da_2021.parquet
#   - load/output/census_demographics_2011.parquet, census_demographics_2021.parquet
#   - load/output/census_economics_2011.parquet, census_economics_2021.parquet
#   - load/output/census_income_2021.parquet  (no 2011 income at DA)
# Writes:
#   - load/output/geo_da.parquet
#   - load/output/census_demographics.parquet
#   - load/output/census_economics.parquet
#   - load/output/census_income.parquet

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTPUT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/output"

echo "Combining year-specific parquet files..."

duckdb -c "
INSTALL spatial; LOAD spatial;

-- ================================================================
-- geo_da: UNION both years
-- ================================================================
COPY (
  SELECT * FROM '$OUTPUT_DIR/geo_da_2011.parquet'
  UNION ALL
  SELECT * FROM '$OUTPUT_DIR/geo_da_2021.parquet'
) TO '$OUTPUT_DIR/geo_da.parquet' (FORMAT PARQUET);

-- ================================================================
-- census_demographics: UNION both years
-- ================================================================
COPY (
  SELECT * FROM '$OUTPUT_DIR/census_demographics_2011.parquet'
  UNION ALL
  SELECT * FROM '$OUTPUT_DIR/census_demographics_2021.parquet'
) TO '$OUTPUT_DIR/census_demographics.parquet' (FORMAT PARQUET);

-- ================================================================
-- census_economics: UNION both years
-- ================================================================
COPY (
  SELECT * FROM '$OUTPUT_DIR/census_economics_2011.parquet'
  UNION ALL
  SELECT * FROM '$OUTPUT_DIR/census_economics_2021.parquet'
) TO '$OUTPUT_DIR/census_economics.parquet' (FORMAT PARQUET);

-- ================================================================
-- census_income: 2021 only (2011 income not available at DA level)
-- ================================================================
COPY (
  SELECT * FROM '$OUTPUT_DIR/census_income_2021.parquet'
) TO '$OUTPUT_DIR/census_income.parquet' (FORMAT PARQUET);
"

# Clean up intermediate year-specific files
rm -f "$OUTPUT_DIR"/geo_da_20*.parquet
rm -f "$OUTPUT_DIR"/census_demographics_20*.parquet
rm -f "$OUTPUT_DIR"/census_economics_20*.parquet
rm -f "$OUTPUT_DIR"/census_income_20*.parquet

echo "Done. Final files:"
for f in geo_da census_demographics census_economics census_income; do
  rows=$(duckdb -c "SELECT COUNT(*) FROM '$OUTPUT_DIR/${f}.parquet'" 2>/dev/null | tail -1 | tr -d ' ')
  echo "  $OUTPUT_DIR/${f}.parquet ($rows rows)"
done
