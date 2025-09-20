# heap_rescan

## Location
[src/backend/access/heap/heapam.c:1196-1253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L1196-L1253)

## Overview
Restarts a heap table scan from the beginning, optionally updating scan parameters and reinitializing the scan state while preserving the scan descriptor.

## Definition

```c
void
heap_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
			bool allow_strat, bool allow_sync, bool allow_pagemode)
```
## Detailed Description
The  function reinitializes an existing heap table scan to start over from the beginning. It handles cleanup of the current scan state, including releasing any pinned buffers, and optionally updates scan parameters before reinitializing the scan. This function is typically used when a query plan needs to restart a table scan, such as in nested loop joins or when implementing resettable scan operations.

The function first updates scan flags based on the provided parameters if  is true, then releases any currently held buffer pins, resets bitmap scan state, resets the read stream if present, and finally calls  to reinitialize the scan from the beginning.

## Parameters / Member Variables
- : The table scan descriptor to rescan (cast to HeapScanDesc internally)
- : New scan key to use for the rescan, or NULL to keep the existing key
- : Whether to update scan parameters (strategy, sync, pagemode flags)
- : Whether to enable synchronized scanning strategy
- : Whether to allow synchronized scanning with other concurrent scans
- : Whether to enable page-at-a-time scanning mode (requires MVCC snapshot)

## Dependencies
- Functions called/Symbols referenced:
  - ReleaseBuffer
  - IsMVCCSnapshot
  - read_stream_reset
  - [initscan](../i/initscan.md)
- Data structures used:
  - [HeapScanDesc](../H/HeapScanDesc.md)
  - [TableScanDesc](../T/TableScanDesc.md)
  - ScanKey
- Scan flags:
  - SO_ALLOW_STRAT
  - SO_ALLOW_SYNC
  - SO_ALLOW_PAGEMODE
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md)
  - HeapScanIsValid

## Notes and Other Information
- The function carefully manages buffer pins by releasing both the current buffer () and visibility map buffer () before reinitializing
- The  field is reset to prevent bitmap heap scans from incorrectly emitting NULL-filled tuples from previous scans
- Read stream state is reset before calling  because some state used by  is modified by 
- Page mode scanning is only enabled if an MVCC snapshot is being used, ensuring consistency requirements are met
- The function preserves the scan descriptor structure while completely reinitializing its scan state