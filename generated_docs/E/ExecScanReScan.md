# ExecScanReScan

## Location
[src/backend/executor/execScan.c:297-345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execScan.c#L297-L345)

## Overview
ExecScanReScan resets scan state for rescanning operations, handling tuple slot clearing and EvalPlanQual state management for all scan node types.

## Definition


## Detailed Description
ExecScanReScan is a critical function called by all scan node types during rescan operations, which occur in nested loop joins, subplan re-evaluation, and other scenarios requiring multiple passes over scan data. The function ensures proper state reset by clearing the current scan tuple slot and managing EvalPlanQual (EPQ) state for concurrent transaction handling.

The function handles two distinct scenarios for EPQ state management: simple scans with a single scanrelid, and complex scans (foreign/custom) that may involve multiple relation IDs due to pushed-down joins. For multiple relations, it iterates through the relation bitmap and resets the EPQ done status while preserving any blocked status, ensuring correct behavior during concurrent updates.

## Parameters / Member Variables
- : The ScanState containing scan execution state, tuple slots, and plan information

## Dependencies
- Functions called/Symbols referenced:
  - ExecClearTuple (clears the scan tuple slot)
  - [bms_next_member](../b/bms_next_member.md) (iterates through relation bitmap for multi-relation scans)
  - IsA (type checking macros for ForeignScan and CustomScan)
  - nodeTag (node type identification)
  - elog (error logging for unexpected scan types)
- Data structures used:
  - [ScanState](../S/ScanState.md)
  - [EState](EState.md)
  - [EPQState](EPQState.md)
  - Scan (plan node)
  - ForeignScan
  - CustomScan
  - [Bitmapset](../B/Bitmapset.md)
- Called from (representative examples):
  - [ExecReScanSeqScan](ExecReScanSeqScan.md) (sequential scan rescan)
  - [ExecReScanIndexScan](ExecReScanIndexScan.md) (index scan rescan)
  - [ExecReScanBitmapHeapScan](ExecReScanBitmapHeapScan.md) (bitmap heap scan rescan)
  - [ExecReScanForeignScan](ExecReScanForeignScan.md) (foreign scan rescan)
  - All other scan node rescan functions

## Notes and Other Information
- This function must be called within the ReScan function of any plan node type that uses ExecScan()
- Clearing the scan tuple slot is essential for observers (like execCurrent.c) to detect that the scan is not positioned on a tuple
- EPQ state management preserves the "blocked" status of target relations while resetting the "done" status for proper concurrent update handling
- Foreign scans and custom scans require special handling due to potentially multiple relation IDs from pushed-down joins
- The function maintains EPQ correctness by ensuring that previously blocked relations remain blocked after rescan
- Error handling ensures that only expected scan node types (including foreign and custom scans) are processed
- The relation ID handling distinguishes between simple scans (scanrelid > 0) and complex multi-relation scans (scanrelid = 0)