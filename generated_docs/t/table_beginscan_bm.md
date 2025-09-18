# table_beginscan_bm

## Location
[src/include/access/tableam.h:954-972](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L954-L972)

## Overview
table_beginscan_bm is a specialized table scanning function designed for bitmap heap scans, providing an optimized scan setup for bitmap-based table access patterns.

## Definition
```c
static inline TableScanDesc
table_beginscan_bm(Relation rel, Snapshot snapshot,
                   int nkeys, struct ScanKeyData *key, bool need_tuple)
```

## Detailed Description
table_beginscan_bm is specifically designed for bitmap heap scans, which represent a hybrid scanning approach between sequential and index scans. While bitmap scans are quite different from standard sequential scans in their access patterns, they share enough commonality to reuse the same TableScanDesc data structure. This function sets up the scan with the SO_TYPE_BITMAPSCAN flag to indicate the specialized scan type and conditionally enables tuple retrieval based on the caller's needs.

Bitmap scans are typically used when an index scan would return a significant portion of the table's rows, making the random access pattern of pure index scans inefficient, but where a full sequential scan would be overkill.

## Parameters / Member Variables
- `rel`: The relation (table) to be scanned using bitmap heap scan
- `snapshot`: Snapshot for visibility checking of tuples during the scan
- `nkeys`: Number of scan keys for filtering (0 means no filtering)
- `key`: Array of ScanKeyData structures defining the filter conditions
- `need_tuple`: Whether the scan needs to actually retrieve tuple data (sets SO_NEED_TUPLES flag)

## Dependencies
- Functions called/Symbols referenced:
  - SO_TYPE_BITMAPSCAN (bitmap scan type flag)
  - SO_ALLOW_PAGEMODE (allows page-at-a-time reading)
  - SO_NEED_TUPLES (conditionally set based on need_tuple parameter)
  - rd_tableam->scan_begin (table access method function)
- Called from (representative examples):
  - [BitmapHeapNext](../B/BitmapHeapNext.md)

## Notes and Other Information
- Specifically designed for bitmap heap scan operations, which combine benefits of index and sequential scans
- The need_tuple parameter allows for optimizations when only tuple existence checking is required
- Bitmap scans are particularly effective for queries that would return a moderate number of rows spread across the table
- Uses a different scan type flag (SO_TYPE_BITMAPSCAN) compared to sequential scans
- The scan leverages bitmap structures created by index scans to optimize page access patterns