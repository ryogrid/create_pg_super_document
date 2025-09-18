# vac_estimate_reltuples

## Location
[src/backend/commands/vacuum.c:1313-1408](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L1313-L1408)

## Overview
Estimates the new value for pg_class.reltuples based on a partial scan of the relation, using tuple density calculations for unscanned pages.

## Definition
```c
double
vac_estimate_reltuples(Relation relation,
                       BlockNumber total_pages,
                       BlockNumber scanned_pages,
                       double scanned_tuples)
```

## Detailed Description
The vac_estimate_reltuples function calculates an accurate estimate of the total number of live tuples in a relation based on partial scan data. When VACUUM scans only a subset of pages, this function extrapolates the total tuple count using the old tuple density from pg_class combined with the new scan results.

The function handles several special cases to avoid estimation distortion: if the entire table was scanned, it returns the actual count; if very few pages were scanned on an unchanged table, it preserves the existing estimate; and if old density data is unavailable, it scales up the scanned count proportionally.

For the normal case, it calculates the old tuple density (tuples per page), applies this density to unscanned pages, and adds the actual count from scanned pages to produce a more accurate total estimate.

## Parameters / Member Variables
- `relation`: The relation being analyzed
- `total_pages`: Total number of pages in the relation
- `scanned_pages`: Number of pages actually scanned during vacuum
- `scanned_tuples`: Number of live tuples found in the scanned pages

## Dependencies
- Functions called/Symbols referenced:
  - floor (standard library function)
- Called from (representative examples):
  - [lazy_scan_heap](../l/lazy_scan_heap.md) (src/backend/access/heap/vacuumlazy.c:1035)

## Notes and Other Information
- Returns estimated total number of live tuples as a double value
- Handles corner cases to prevent estimation drift from repeated partial scans
- Uses existing pg_class.reltuples and relpages values as baseline density measurements
- Prevents distortion when scanning the same small subset of pages repeatedly
- Only counts live tuples, consistent with pg_class.reltuples definition
- May return -1 in cases where the old estimate was -1 (unknown)
- Used to update pg_class.reltuples after vacuum operations
- Location: src/backend/commands/vacuum.c:1313-1408