# ExecReScanBitmapHeapScan

## Location
src/backend/executor/nodeBitmapHeapscan.c: 595 - 639

## Overview
ExecReScanBitmapHeapScan resets a bitmap heap scan node to restart scanning from the beginning, cleaning up all associated resources including bitmaps, iterators, and buffers, then rescanning the outer plan if needed.

## Definition
void ExecReScanBitmapHeapScan(BitmapHeapScanState *node)

## Detailed Description
ExecReScanBitmapHeapScan is responsible for resetting a bitmap heap scan node to its initial state so that scanning can restart from the beginning. This function performs comprehensive cleanup of all resources associated with the bitmap heap scan, including closing table scan descriptors, releasing bitmap iterators (both regular and shared versions), freeing bitmap memory structures, and releasing buffer pins.

The function also handles the coordination with the outer plan (typically a bitmap index scan) by calling ExecReScan on it if the outer plan's parameters haven't changed. This ensures that both the bitmap creation phase and the heap scanning phase are properly reset. The rescan operation is essential for implementing nested loops, parameter changes in prepared statements, and other scenarios where a scan needs to be repeated.

## Parameters / Member Variables
- : BitmapHeapScanState pointer containing the bitmap heap scan execution state that needs to be reset to its initial scanning state

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState (macro to get outer plan)
  - [table_rescan](../t/table_rescan.md)
  - [tbm_end_iterate](../t/tbm_end_iterate.md)
  - [tbm_end_shared_iterate](../t/tbm_end_shared_iterate.md)  
  - [tbm_free](../t/tbm_free.md)
  - ReleaseBuffer
  - [ExecScanReScan](ExecScanReScan.md)
  - [ExecReScan](ExecReScan.md)
- Data types referenced:
  - [BitmapHeapScanState](../B/BitmapHeapScanState.md)
  - [PlanState](../P/PlanState.md)
- Called from:
  - [ExecReScan](ExecReScan.md) (src/backend/executor/execAmi.c:193)
- Referenced in headers:
  - src/include/executor/nodeBitmapHeapscan.h:22

## Notes and Other Information
- This is a public function (not static) as it needs to be callable from the generic executor rescan infrastructure
- The function performs extensive resource cleanup including both regular and shared bitmap iterators for parallel query support
- Releases page pins by calling table_rescan with NULL parameters on the current scan descriptor
- Resets the initialized flag to false, forcing reinitialization on the next execution
- Handles both parallel (shared) and non-parallel bitmap iterators and resources
- The function checks if outer plan parameters have changed (chgParam) to avoid unnecessary rescans of the outer plan
- Part of the standard executor node interface for rescan operations
- Critical for memory management and proper resource cleanup in bitmap heap scans