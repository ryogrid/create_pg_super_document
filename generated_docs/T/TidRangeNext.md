# TidRangeNext

## Location
[src/backend/executor/nodeTidrangescan.c:220-272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTidrangescan.c#L220-L272)

## Overview
TidRangeNext retrieves the next tuple from a TID range scan by managing the scan descriptor and coordinating with the table access method to fetch tuples within the computed TID range.

## Definition


## Detailed Description
This function implements the core tuple retrieval logic for TID range scans. On the first call, it evaluates the TID range using TidRangeEval and initializes or updates the table scan descriptor with the computed range bounds. For subsequent calls, it simply fetches the next tuple from the ongoing scan. The function handles both initial scan setup and scan continuation, managing the scan state appropriately. When no more tuples are available in the range, it marks the scan as complete and clears the tuple slot.

## Parameters / Member Variables
- `node`: TidRangeScanState containing scan state, range bounds, and relation information

## Dependencies
- Functions called/Symbols referenced:
  - [TidRangeEval](TidRangeEval.md)
  - table_beginscan_tidrange
  - table_rescan_tidrange
  - table_scan_getnextslot_tidrange
  - ExecClearTuple
- Data structures used:
  - [TableScanDesc](TableScanDesc.md)
  - [EState](../E/EState.md)
  - ScanDirection
  - TupleTableSlot
- Called from:
  - [ExecTidRangeScan](../E/ExecTidRangeScan.md)

## Notes and Other Information
- Returns NULL if TidRangeEval determines no tuples can match the range criteria
- Returns a TupleTableSlot containing the next tuple, or an empty slot if scan is complete
- The trss_inScan flag tracks whether the scan is currently active
- Supports both forward and backward scan directions based on estate->es_direction
- Handles both initial scan setup and rescan scenarios
- The scan descriptor is reused across multiple calls for efficiency
- Automatically manages scan state transitions and cleanup