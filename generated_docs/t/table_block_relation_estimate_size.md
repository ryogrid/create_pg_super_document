# table_block_relation_estimate_size

## Location
[src/backend/access/table/tableam.c:654-761](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/tableam.c#L654-L761)

## Overview
A helper function that estimates the size characteristics of a relation (pages, tuples, all-visible fraction) for query planning purposes, handling both cases with and without existing statistics.

## Definition
```c
void table_block_relation_estimate_size(Relation rel, int32 *attr_widths,
                                        BlockNumber *pages, double *tuples,
                                        double *allvisfrac,
                                        Size overhead_bytes_per_tuple,
                                        Size usable_bytes_per_page)
```

## Detailed Description
This function provides size estimation logic for table access methods by combining current physical storage information with historical statistics from pg_class. It serves as a helper that can be called by table AM implementations rather than requiring each AM to implement estimation logic from scratch.

The function handles two primary scenarios: when reliable tuple density statistics exist (reltuples >= 0 and relpages > 0), it uses historical density; when no statistics are available, it estimates tuple width from attribute types and applies fillfactor considerations.

A key optimization is the "10-page minimum" heuristic for never-vacuumed tables to avoid overly optimistic plans for tables that may grow significantly. This prevents nestloop plans from being chosen for tables that appear small initially but will expand once populated.

The all-visible fraction calculation assumes newly added pages since the last VACUUM are not marked all-visible, providing conservative estimates for query planning.

## Parameters / Member Variables
- `rel`: The relation to estimate
- `attr_widths`: Array of attribute width estimates for tuple width calculation
- `pages`: Output parameter for estimated number of pages
- `tuples`: Output parameter for estimated number of tuples
- `allvisfrac`: Output parameter for fraction of all-visible pages
- `overhead_bytes_per_tuple`: Storage overhead per tuple (headers, item pointers, etc.)
- `usable_bytes_per_page`: Available space per page for tuple data

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfBlocks
  - RelationGetFillFactor
  - [get_rel_data_width](../g/get_rel_data_width.md)
  - [clamp_row_est](../c/clamp_row_est.md)
  - HEAP_DEFAULT_FILLFACTOR
- Called from (representative examples):
  - [heapam_estimate_rel_size](../h/heapam_estimate_rel_size.md)
  - table_scan_sample_next_tuple

## Notes and Other Information
- Cannot be used directly as a relation_estimate_size callback due to additional parameters
- Implements the "never vacuumed" heuristic using reltuples < 0 as the indicator
- Intentionally ignores alignment considerations for cross-platform consistency
- Uses integer division deliberately for density calculation
- Applies clamp_row_est to ensure at least one tuple per page
- Part of the table access method infrastructure for query planning optimization