# ExecReScanTidRangeScan

## Location
[src/backend/executor/nodeTidrangescan.c:308-326](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTidrangescan.c#L308-L326)

## Overview
ExecReScanTidRangeScan resets a TID range scan to its initial state, preparing it to be re-executed from the beginning without performing the actual table rescan until needed.

## Definition
```c
void ExecReScanTidRangeScan(TidRangeScanState *node)
```

## Detailed Description
ExecReScanTidRangeScan is responsible for resetting the state of a TID range scan operation to allow it to be re-executed from the beginning. The function follows a lazy approach to rescanning - it marks the scan as not in progress and resets the scan state without immediately calling the table rescan function. The actual table rescan operation is deferred until TidRangeNext is called, which optimizes performance by avoiding unnecessary work if the scan is never actually resumed.

The function sets the `trss_inScan` flag to false, indicating that the scan is not currently in progress, and delegates the generic rescan operations to ExecScanReScan which handles common rescan tasks for all scan types.

## Parameters / Member Variables
- `node`: A TidRangeScanState pointer containing the execution state for the TID range scan that needs to be reset

## Dependencies
- Functions called/Symbols referenced:
  - [ExecScanReScan](ExecScanReScan.md) (generic scan rescan functionality)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (generic plan node rescan dispatcher)

## Notes and Other Information
- The function implements lazy rescanning - [table_rescan_tidrange](../t/table_rescan_tidrange.md) is not called until TidRangeNext executes
- The `trss_inScan` flag is used to track whether a scan is currently active
- This function is part of the standard PostgreSQL executor interface for rescan operations
- The deferred rescan approach helps optimize cases where a rescan is requested but the scan may not actually be resumed