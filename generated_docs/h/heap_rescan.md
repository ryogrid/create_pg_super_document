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
- `sscan`: The table scan descriptor to rescan (cast to HeapScanDesc internally)
- `key`: New scan key to use for the rescan, or NULL to keep the existing key
- `set_params`: Whether to update scan parameters (strategy, sync, pagemode flags)
- `allow_strat`: Whether to enable synchronized scanning strategy
- `allow_sync`: Whether to allow synchronized scanning with other concurrent scans
- `allow_pagemode`: Whether to enable page-at-a-time scanning mode (requires MVCC snapshot)
## Dependencies
- Functions called/Symbols referenced:
  - [ReleaseBuffer](../R/ReleaseBuffer.md)
  - IsMVCCSnapshot
  - [read_stream_reset](../r/read_stream_reset.md)
  - [initscan](../i/initscan.md)
- Data structures used:
  - [HeapScanDesc](../H/HeapScanDesc.md)
  - [TableScanDesc](../T/TableScanDesc.md)
  - ScanKey
- [Scan](../S/Scan.md) flags:
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

## Simplified Source

```c
void heap_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
               bool allow_strat, bool allow_sync, bool allow_pagemode) {
    HeapScanDesc scan = (HeapScanDesc) sscan;

    // Update scan parameters if requested
    if (set_params) {
        if (allow_strat)
            scan->rs_base.rs_flags |= SO_ALLOW_STRAT;
        else
            scan->rs_base.rs_flags &= ~SO_ALLOW_STRAT;

        if (allow_sync)
            scan->rs_base.rs_flags |= SO_ALLOW_SYNC;
        else
            scan->rs_base.rs_flags &= ~SO_ALLOW_SYNC;

        if (allow_pagemode && scan->rs_base.rs_snapshot &&
            IsMVCCSnapshot(scan->rs_base.rs_snapshot))
            scan->rs_base.rs_flags |= SO_ALLOW_PAGEMODE;
        else
            scan->rs_base.rs_flags &= ~SO_ALLOW_PAGEMODE;
    }

    // Release scan buffers
    if (BufferIsValid(scan->rs_cbuf))
        ReleaseBuffer(scan->rs_cbuf);

    if (BufferIsValid(scan->rs_vmbuffer)) {
        ReleaseBuffer(scan->rs_vmbuffer);
        scan->rs_vmbuffer = InvalidBuffer;
    }

    // Reset bitmap scan state
    scan->rs_empty_tuples_pending = 0;

    // Reset read stream before reinitializing scan
    if (scan->rs_read_stream)
        read_stream_reset(scan->rs_read_stream);

    // Reinitialize scan from beginning
    initscan(scan, key, true);
}
```