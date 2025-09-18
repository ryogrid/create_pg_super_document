# spgrescan

## Location
src/backend/access/spgist/spgscan.c: 380 - 428

## Overview
Resets and restarts an SP-GiST index scan with new scan keys and order-by conditions, preparing the scan state for a fresh traversal.

## Definition
```c
void spgrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
               ScanKey orderbys, int norderbys)
```

## Detailed Description
This function prepares an existing SP-GiST index scan for reuse with potentially new scan conditions and ordering requirements. It copies the provided scan keys and order-by conditions into the scan descriptor, processes the scan keys to handle null-related logic, and resets the scan state to start fresh from the root.

For distance-ordered scans, it determines the return types of the ordering operators by looking up the original ordering operator's result type. The function ensures proper statistics tracking by counting the index scan operation.

## Parameters / Member Variables
- `scan`: IndexScanDesc structure representing the index scan
- `scankey`: Array of scan keys (search conditions) to use
- `nscankeys`: Number of scan keys provided (currently unused but part of interface)
- `orderbys`: Array of order-by conditions for distance-based searches
- `norderbys`: Number of order-by conditions provided (currently unused but part of interface)

## Dependencies
- Functions called/Symbols referenced:
  - memmove (for copying scan keys)
  - [get_func_rettype](../g/get_func_rettype.md)
  - [spgPrepareScanKeys](spgPrepareScanKeys.md)
  - [resetSpGistScanOpaque](../r/resetSpGistScanOpaque.md)
  - pgstat_count_index_scan
- Called from:
  - [spghandler](spghandler.md) (src/backend/access/spgist/spgutils.c:85)

## Dependencies
- Types used:
  - [IndexScanDesc](../I/IndexScanDesc.md)
  - ScanKey
  - SpGistScanOpaque

## Notes and Other Information
- The function handles both regular scans and distance-ordered scans
- For order-by operations, SP-GiST uses float8 for distance calculations internally, but the ordering operator can return any type
- Distance functions can only be lossy if the ordering operator returns float4 or float8 types
- The function processes scan keys through spgPrepareScanKeys to separate null/non-null search logic
- Calls resetSpGistScanOpaque to clear previous scan state and initialize the search queue
- Updates PostgreSQL statistics by counting the index scan operation
- This is the function called to start or restart scanning after spgbeginscan has been called