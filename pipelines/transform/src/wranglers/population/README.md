# Population wranglers

Population data for Montreal's arrondissements and demerged municipalities,
sourced from Statistics Canada (StatCan) census profiles and Ville de
Montreal open data.

## Data sources

| Year | Source | Notes |
|------|--------|-------|
| 1991 | StatCan per-CSD profiles | Pre-demerger boundaries |
| 1996 | StatCan per-CSD profiles | Pre-demerger boundaries |
| 2001 | StatCan per-CSD profiles | Post-demerger municipalities only |
| 2006 | StatCan per-CSD + Ville de Montreal | Hybrid (see below) |
| 2011 | StatCan per-CSD + Ville de Montreal | Hybrid |
| 2016 | StatCan bulk CSV + Ville de Montreal | Hybrid |
| 2021 | StatCan bulk CSV + Ville de Montreal | Hybrid |

## Why two sources (2006-2021)?

StatCan publishes census profiles per Census Subdivision (CSD). In the
Montreal agglomeration, each **demerged municipality** (Westmount, Dorval,
Mont-Royal, etc.) is its own CSD, so StatCan provides their population
directly.

However, the **arrondissements** of the Ville de Montreal (Rosemont,
Villeray, Plateau, etc.) are all sub-divisions of a single CSD (2466023 =
"Montreal"). StatCan does not break that CSD into arrondissements. For
arrondissement-level data we rely on Ville de Montreal's
`population_mtl_by_district.csv` files.

### Hybrid wrangler pattern

Each year from 2006 onward exports three functions:

- `wrangle_YYYY_statcan()` -- reads StatCan per-CSD data for demerged
  municipalities. Each row gets `source = "StatCan YYYY Census profiles"`.
- `wrangle_YYYY_mtl()` -- reads Ville de Montreal data for all districts.
  Each row gets `source = "Ville de Montreal"`.
- `wrangle_YYYY()` -- combines the two: takes all StatCan rows, then
  appends only the Ville de Montreal rows whose `arrondissement` is not
  already in the StatCan set (i.e., the arrondissements).

This gives us the best of both worlds: official StatCan figures for
demerged municipalities and Ville de Montreal figures for arrondissements.

### Cross-validation

The test suite (`pipelines/transform/tests/test_metadata.R`) cross-validates
StatCan against Ville de Montreal for every year where both sources cover the
same districts. For demerged municipalities, both sources report the same
populations (Tests 3-6).

## StatCan data formats

| Year | Format | Key columns |
|------|--------|-------------|
| 2001 | Per-CSD CSV (one file per CSD) | Line 2 = geography name, line 3 = population |
| 2006 | Per-CSD CSV (one file per CSD) | Line 4 = "Population, 2006" row |
| 2011 | Per-CSD CSV (columnar) | `Characteristics == "Population in 2011"`, col 4 |
| 2016 | Bulk Quebec CSV (pre-extracted to 16 rows) | Col 4 = `GEO_NAME`, col 13 = total population |
| 2021 | Bulk Quebec CSV (pre-extracted to 16 rows) | `GEO_NAME` (with CSD type suffix), `C1_COUNT_TOTAL` |

For 2016 and 2021, the raw StatCan downloads are large ZIP archives
containing all Quebec CSDs. The ingest pipeline (`census.R`) uses the geo
starting row index to extract only the 16 Montreal-area CSDs into a small
CSV (`montreal_population_YYYY.csv`).

## Montreal CSD codes

The 16 CSDs in the Montreal agglomeration (consistent across 2011-2021):

```
2466007  2466023  2466032  2466047  2466058  2466062  2466072  2466087
2466092  2466097  2466102  2466107  2466112  2466117  2466127  2466142
```

CSD 2466023 is "Montreal" proper (the arrondissements). The remaining 15 are
the demerged municipalities.
