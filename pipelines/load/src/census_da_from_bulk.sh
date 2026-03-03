#!/bin/bash
# Build DA-level census star schema from StatCan bulk CSV + DA boundary shapefile.
#
# Reads:
#   - ingest/input/census_da/2021/quebec_da_2021/*_English_CSV_data_Quebec.csv
#   - ingest/input/geo/da_boundary_2021/*.shp
# Writes:
#   - load/output/geo_da.parquet              (geometry layer)
#   - load/output/census_demographics.parquet  (age × sex fact table)
#   - load/output/census_economics.parquet     (generic measures fact table)
#   - load/output/census_income.parquet        (income distribution fact table)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INGEST_DIR="$(cd "$SCRIPT_DIR/../../ingest/input" && pwd)"
OUTPUT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/output"
mkdir -p "$OUTPUT_DIR"

# Locate input files (names vary by StatCan release)
# The ZIP splits by region — we need the Quebec file for Montreal DAs
DA_CSV=$(find "$INGEST_DIR/census_da/2021/quebec_da_2021" -name '*_English_CSV_data_Quebec.csv' | head -1)
DA_SHP=$(find "$INGEST_DIR/geo/da_boundary_2021" -name '*.shp' | head -1)

if [ -z "$DA_CSV" ]; then
  echo "Error: DA bulk CSV not found in $INGEST_DIR/census_da/2021/quebec_da_2021/"
  exit 1
fi
if [ -z "$DA_SHP" ]; then
  echo "Error: DA boundary shapefile not found in $INGEST_DIR/geo/da_boundary_2021/"
  exit 1
fi

echo "Building star schema from bulk CSV..."
echo "  CSV: $DA_CSV"
echo "  SHP: $DA_SHP"

duckdb -c "
INSTALL spatial; LOAD spatial;

-- ================================================================
-- 1. geo_da.parquet — geometry layer (one row per DA)
-- ================================================================
COPY (
  SELECT
    b.DAUID    AS geo_uid,
    b.DAUID    AS da_name,
    b.LANDAREA AS area_sqkm,
    b.geom
  FROM ST_Read('$DA_SHP') b
  -- Filter to Montreal DAs that have population > 0
  WHERE b.DAUID LIKE '2466%'
    AND b.DAUID IN (
      SELECT ALT_GEO_CODE
      FROM read_csv('$DA_CSV', all_varchar=true, encoding='latin-1')
      WHERE ALT_GEO_CODE LIKE '2466%'
        AND CAST(CHARACTERISTIC_ID AS INT) = 1
        AND TRY_CAST(C1_COUNT_TOTAL AS DOUBLE) > 0
    )
) TO '$OUTPUT_DIR/geo_da.parquet' (FORMAT PARQUET);

-- ================================================================
-- 2. census_demographics.parquet — full age pyramid (long format)
--    21 five-year age groups × 3 sexes per DA
-- ================================================================
COPY (
  WITH age_data AS (
    SELECT
      ALT_GEO_CODE AS geo_uid,
      CAST(CHARACTERISTIC_ID AS INT) AS cid,
      TRY_CAST(C1_COUNT_TOTAL AS DOUBLE) AS total,
      TRY_CAST(\"C2_COUNT_MEN+\" AS DOUBLE) AS male,
      TRY_CAST(\"C3_COUNT_WOMEN+\" AS DOUBLE) AS female
    FROM read_csv('$DA_CSV', all_varchar=true, encoding='latin-1')
    WHERE ALT_GEO_CODE LIKE '2466%'
      AND CAST(CHARACTERISTIC_ID AS INT) IN (10,11,12,14,15,16,17,18,19,20,21,22,23,25,26,27,28,30,31,32,33)
  ),
  labeled AS (
    SELECT geo_uid,
      CASE cid
        WHEN 10 THEN '0-4'   WHEN 11 THEN '5-9'   WHEN 12 THEN '10-14'
        WHEN 14 THEN '15-19' WHEN 15 THEN '20-24'  WHEN 16 THEN '25-29'
        WHEN 17 THEN '30-34' WHEN 18 THEN '35-39'  WHEN 19 THEN '40-44'
        WHEN 20 THEN '45-49' WHEN 21 THEN '50-54'  WHEN 22 THEN '55-59'
        WHEN 23 THEN '60-64' WHEN 25 THEN '65-69'  WHEN 26 THEN '70-74'
        WHEN 27 THEN '75-79' WHEN 28 THEN '80-84'  WHEN 30 THEN '85-89'
        WHEN 31 THEN '90-94' WHEN 32 THEN '95-99'  WHEN 33 THEN '100+'
      END AS age_group,
      total, male, female
    FROM age_data
  )
  SELECT geo_uid, age_group, 'total' AS sex, total AS population FROM labeled
  UNION ALL
  SELECT geo_uid, age_group, 'male' AS sex, male AS population FROM labeled
  UNION ALL
  SELECT geo_uid, age_group, 'female' AS sex, female AS population FROM labeled
) TO '$OUTPUT_DIR/census_demographics.parquet' (FORMAT PARQUET);

-- ================================================================
-- 3. census_economics.parquet — generic measures (long format)
--    (geo_uid, variable, sex, value)
-- ================================================================
COPY (
  WITH raw AS (
    SELECT
      ALT_GEO_CODE AS geo_uid,
      CAST(CHARACTERISTIC_ID AS INT) AS cid,
      TRY_CAST(C1_COUNT_TOTAL AS DOUBLE) AS c1,
      TRY_CAST(\"C2_COUNT_MEN+\" AS DOUBLE) AS c2,
      TRY_CAST(\"C3_COUNT_WOMEN+\" AS DOUBLE) AS c3
    FROM read_csv('$DA_CSV', all_varchar=true, encoding='latin-1')
    WHERE ALT_GEO_CODE LIKE '2466%'
      AND CAST(CHARACTERISTIC_ID AS INT) IN (1,4,39,243,244,252,253,318,319,333,396,397,1415,1416,1529,340,345,355,360)
  )
  -- population & dwellings
  SELECT geo_uid, 'pop_total' AS variable, 'total' AS sex, c1 AS value FROM raw WHERE cid = 1
  UNION ALL
  SELECT geo_uid, 'dwellings_total', 'total', c1 FROM raw WHERE cid = 4
  UNION ALL
  SELECT geo_uid, 'avg_age', 'total', c1 FROM raw WHERE cid = 39
  -- income (household — no sex split)
  UNION ALL
  SELECT geo_uid, 'median_income_household', 'total', c1 FROM raw WHERE cid = 243
  UNION ALL
  SELECT geo_uid, 'median_income_aftertax_household', 'total', c1 FROM raw WHERE cid = 244
  UNION ALL
  SELECT geo_uid, 'avg_income_household', 'total', c1 FROM raw WHERE cid = 252
  UNION ALL
  SELECT geo_uid, 'avg_income_aftertax_household', 'total', c1 FROM raw WHERE cid = 253
  -- income (person — C1=total, C2=male, C3=female)
  UNION ALL
  SELECT geo_uid, 'median_income', 'total', c1 FROM raw WHERE cid = 318
  UNION ALL
  SELECT geo_uid, 'median_income', 'male', c2 FROM raw WHERE cid = 318
  UNION ALL
  SELECT geo_uid, 'median_income', 'female', c3 FROM raw WHERE cid = 318
  UNION ALL
  SELECT geo_uid, 'median_income_aftertax', 'total', c1 FROM raw WHERE cid = 319
  UNION ALL
  SELECT geo_uid, 'median_income_aftertax', 'male', c2 FROM raw WHERE cid = 319
  UNION ALL
  SELECT geo_uid, 'median_income_aftertax', 'female', c3 FROM raw WHERE cid = 319
  UNION ALL
  SELECT geo_uid, 'avg_income', 'total', c1 FROM raw WHERE cid = 333
  UNION ALL
  SELECT geo_uid, 'avg_income', 'male', c2 FROM raw WHERE cid = 333
  UNION ALL
  SELECT geo_uid, 'avg_income', 'female', c3 FROM raw WHERE cid = 333
  -- housing tenure
  UNION ALL
  SELECT geo_uid, 'tenure_owner', 'total', c1 FROM raw WHERE cid = 1415
  UNION ALL
  SELECT geo_uid, 'tenure_renter', 'total', c1 FROM raw WHERE cid = 1416
  -- language
  UNION ALL
  SELECT geo_uid, 'lang_mother_english', 'total', c1 FROM raw WHERE cid = 396
  UNION ALL
  SELECT geo_uid, 'lang_mother_french', 'total', c1 FROM raw WHERE cid = 397
  -- immigration
  UNION ALL
  SELECT geo_uid, 'pop_immigrant', 'total', c1 FROM raw WHERE cid = 1529
  -- low income measures
  UNION ALL
  SELECT geo_uid, 'lim_at_count', 'total', c1 FROM raw WHERE cid = 340
  UNION ALL
  SELECT geo_uid, 'lim_at_count', 'male', c2 FROM raw WHERE cid = 340
  UNION ALL
  SELECT geo_uid, 'lim_at_count', 'female', c3 FROM raw WHERE cid = 340
  UNION ALL
  SELECT geo_uid, 'lim_at_prevalence', 'total', c1 FROM raw WHERE cid = 345
  UNION ALL
  SELECT geo_uid, 'lico_at_count', 'total', c1 FROM raw WHERE cid = 355
  UNION ALL
  SELECT geo_uid, 'lico_at_count', 'male', c2 FROM raw WHERE cid = 355
  UNION ALL
  SELECT geo_uid, 'lico_at_count', 'female', c3 FROM raw WHERE cid = 355
  UNION ALL
  SELECT geo_uid, 'lico_at_prevalence', 'total', c1 FROM raw WHERE cid = 360
) TO '$OUTPUT_DIR/census_economics.parquet' (FORMAT PARQUET);

-- ================================================================
-- 4. census_income.parquet — income distribution brackets (long format)
--    (geo_uid, income_type, bracket, sex, count)
--    Person-level brackets have C1/C2/C3; household brackets C1 only.
-- ================================================================
COPY (
  WITH raw AS (
    SELECT
      ALT_GEO_CODE AS geo_uid,
      CAST(CHARACTERISTIC_ID AS INT) AS cid,
      TRY_CAST(C1_COUNT_TOTAL AS DOUBLE) AS c1,
      TRY_CAST(\"C2_COUNT_MEN+\" AS DOUBLE) AS c2,
      TRY_CAST(\"C3_COUNT_WOMEN+\" AS DOUBLE) AS c3
    FROM read_csv('$DA_CSV', all_varchar=true, encoding='latin-1')
    WHERE ALT_GEO_CODE LIKE '2466%'
      AND CAST(CHARACTERISTIC_ID AS INT) IN (
        -- person total income (leaf brackets)
        156,158,159,160,161,162,163,164,165,166,167,169,170,
        -- person after-tax income (leaf brackets)
        172,174,175,176,177,178,179,180,181,182,183,185,186,
        -- household total income (leaf brackets)
        261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,277,278,279,280,
        -- household after-tax income (leaf brackets)
        282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,298,299,300
      )
  ),
  labeled AS (
    SELECT geo_uid, cid, c1, c2, c3,
      CASE
        WHEN cid BETWEEN 155 AND 170 THEN 'total_income'
        WHEN cid BETWEEN 171 AND 186 THEN 'aftertax_income'
        WHEN cid BETWEEN 260 AND 280 THEN 'household_total_income'
        WHEN cid BETWEEN 281 AND 300 THEN 'household_aftertax_income'
      END AS income_type,
      CASE cid
        -- person total income (13 leaf brackets)
        WHEN 156 THEN 'Without income'
        WHEN 158 THEN 'Under \$10,000'
        WHEN 159 THEN '\$10,000-\$19,999'
        WHEN 160 THEN '\$20,000-\$29,999'
        WHEN 161 THEN '\$30,000-\$39,999'
        WHEN 162 THEN '\$40,000-\$49,999'
        WHEN 163 THEN '\$50,000-\$59,999'
        WHEN 164 THEN '\$60,000-\$69,999'
        WHEN 165 THEN '\$70,000-\$79,999'
        WHEN 166 THEN '\$80,000-\$89,999'
        WHEN 167 THEN '\$90,000-\$99,999'
        WHEN 169 THEN '\$100,000-\$149,999'
        WHEN 170 THEN '\$150,000+'
        -- person after-tax income (13 leaf brackets)
        WHEN 172 THEN 'Without income'
        WHEN 174 THEN 'Under \$10,000'
        WHEN 175 THEN '\$10,000-\$19,999'
        WHEN 176 THEN '\$20,000-\$29,999'
        WHEN 177 THEN '\$30,000-\$39,999'
        WHEN 178 THEN '\$40,000-\$49,999'
        WHEN 179 THEN '\$50,000-\$59,999'
        WHEN 180 THEN '\$60,000-\$69,999'
        WHEN 181 THEN '\$70,000-\$79,999'
        WHEN 182 THEN '\$80,000-\$89,999'
        WHEN 183 THEN '\$90,000-\$99,999'
        WHEN 185 THEN '\$100,000-\$124,999'
        WHEN 186 THEN '\$125,000+'
        -- household total income (19 leaf brackets)
        WHEN 261 THEN 'Under \$5,000'
        WHEN 262 THEN '\$5,000-\$9,999'
        WHEN 263 THEN '\$10,000-\$14,999'
        WHEN 264 THEN '\$15,000-\$19,999'
        WHEN 265 THEN '\$20,000-\$24,999'
        WHEN 266 THEN '\$25,000-\$29,999'
        WHEN 267 THEN '\$30,000-\$34,999'
        WHEN 268 THEN '\$35,000-\$39,999'
        WHEN 269 THEN '\$40,000-\$44,999'
        WHEN 270 THEN '\$45,000-\$49,999'
        WHEN 271 THEN '\$50,000-\$59,999'
        WHEN 272 THEN '\$60,000-\$69,999'
        WHEN 273 THEN '\$70,000-\$79,999'
        WHEN 274 THEN '\$80,000-\$89,999'
        WHEN 275 THEN '\$90,000-\$99,999'
        WHEN 277 THEN '\$100,000-\$124,999'
        WHEN 278 THEN '\$125,000-\$149,999'
        WHEN 279 THEN '\$150,000-\$199,999'
        WHEN 280 THEN '\$200,000+'
        -- household after-tax income (18 leaf brackets)
        WHEN 282 THEN 'Under \$5,000'
        WHEN 283 THEN '\$5,000-\$9,999'
        WHEN 284 THEN '\$10,000-\$14,999'
        WHEN 285 THEN '\$15,000-\$19,999'
        WHEN 286 THEN '\$20,000-\$24,999'
        WHEN 287 THEN '\$25,000-\$29,999'
        WHEN 288 THEN '\$30,000-\$34,999'
        WHEN 289 THEN '\$35,000-\$39,999'
        WHEN 290 THEN '\$40,000-\$44,999'
        WHEN 291 THEN '\$45,000-\$49,999'
        WHEN 292 THEN '\$50,000-\$59,999'
        WHEN 293 THEN '\$60,000-\$69,999'
        WHEN 294 THEN '\$70,000-\$79,999'
        WHEN 295 THEN '\$80,000-\$89,999'
        WHEN 296 THEN '\$90,000-\$99,999'
        WHEN 298 THEN '\$100,000-\$124,999'
        WHEN 299 THEN '\$125,000-\$149,999'
        WHEN 300 THEN '\$150,000+'
      END AS bracket
    FROM raw
  )
  -- person-level: 3 sexes
  SELECT geo_uid, income_type, bracket, 'total' AS sex, c1 AS count
  FROM labeled WHERE income_type IN ('total_income', 'aftertax_income')
  UNION ALL
  SELECT geo_uid, income_type, bracket, 'male' AS sex, c2 AS count
  FROM labeled WHERE income_type IN ('total_income', 'aftertax_income')
  UNION ALL
  SELECT geo_uid, income_type, bracket, 'female' AS sex, c3 AS count
  FROM labeled WHERE income_type IN ('total_income', 'aftertax_income')
  UNION ALL
  -- household-level: total only
  SELECT geo_uid, income_type, bracket, 'total' AS sex, c1 AS count
  FROM labeled WHERE income_type IN ('household_total_income', 'household_aftertax_income')
) TO '$OUTPUT_DIR/census_income.parquet' (FORMAT PARQUET);
"

echo "Done:"
echo "  $OUTPUT_DIR/geo_da.parquet"
echo "  $OUTPUT_DIR/census_demographics.parquet"
echo "  $OUTPUT_DIR/census_economics.parquet"
echo "  $OUTPUT_DIR/census_income.parquet"
