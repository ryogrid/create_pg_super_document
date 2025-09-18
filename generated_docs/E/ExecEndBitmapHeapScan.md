# ExecEndBitmapHeapScan

## Location
[src/backend/executor/nodeBitmapHeapscan.c:640-684](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeBitmapHeapscan.c#L640-L684)

## Overview
ExecEndBitmapHeapScan terminates a bitmap heap scan node by cleaning up all associated resources including closing table scans, freeing bitmaps and iterators, releasing buffers, and shutting down child plans.

## Definition
void ExecEndBitmapHeapScan(BitmapHeapScanState *node)

## Detailed Description
ExecEndBitmapHeapScan performs the final cleanup and termination of a bitmap heap scan node during query execution shutdown. This function is responsible for properly deallocating all resources that were acquired during the scan's lifetime, including bitmap memory structures, iterator objects (both regular and shared for parallel processing), buffer pins, and table scan descriptors.

The function follows a systematic cleanup approach: first it shuts down child nodes (typically bitmap index scan nodes) by calling ExecEndNode on the outer plan, then releases all bitmap-related resources including iterators and memory structures, releases any held buffer pins, and finally closes the heap table scan descriptor. This ensures that no resources are leaked when the query execution completes.

## Parameters / Member Variables
- : BitmapHeapScanState pointer containing the bitmap heap scan execution state and all associated resources that need to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [ExecEndNode](ExecEndNode.md)
  - outerPlanState (macro to get outer plan)
  - [tbm_end_iterate](../t/tbm_end_iterate.md)
  - [tbm_end_shared_iterate](../t/tbm_end_shared_iterate.md)
  - [tbm_free](../t/tbm_free.md)
  - ReleaseBuffer
  - [table_endscan](../t/table_endscan.md)
- Data types referenced:
  - [BitmapHeapScanState](../B/BitmapHeapScanState.md)
  - [TableScanDesc](../T/TableScanDesc.md)
- Called from:
  - [ExecEndNode](ExecEndNode.md) (src/backend/executor/execProcnode.c:647)
- Referenced in headers:
  - src/include/executor/nodeBitmapHeapscan.h:21

## Notes and Other Information
- This is a public function (not static) as it needs to be callable from the generic executor node termination infrastructure
- The function handles cleanup for both parallel and non-parallel bitmap scan resources
- Properly manages memory by freeing bitmap structures and ending iterator sessions before releasing buffer pins
- Calls ExecEndNode on the outer plan to ensure proper cascading shutdown of the entire plan subtree
- The function checks for NULL/invalid values before attempting to free resources, making it safe to call multiple times
- Part of the standard executor node interface for cleanup operations
- Critical for preventing memory leaks and resource exhaustion in bitmap heap scans
- The cleanup order is important: child nodes are terminated first, then bitmaps and iterators, then buffers, and finally the table scan