# heap_endscan

## Location
[src/backend/access/heap/heapam.c:1254-1295](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L1254-L1295)

## Overview
Terminates a heap table scan by releasing all associated resources including buffers, access strategies, and scan descriptors.

## Definition

```c
void
heap_endscan(TableScanDesc sscan)
```
## Detailed Description
The  function performs cleanup operations to terminate a heap table scan and free all associated resources. This includes releasing any pinned buffers, ending read streams, decrementing relation reference counts, freeing memory allocations, and cleaning up temporary snapshots. The function ensures proper resource management by releasing resources in the correct order - notably freeing the read stream before the BufferAccessStrategy to avoid dependency issues.

## Parameters / Member Variables
- `sscan`: The table scan descriptor to terminate (cast to HeapScanDesc internally)
## Dependencies
- Functions called/Symbols referenced:
  - [ReleaseBuffer](../R/ReleaseBuffer.md)
  - [read_stream_end](../r/read_stream_end.md)
  - [RelationDecrementReferenceCount](../R/RelationDecrementReferenceCount.md)
  - [pfree](../p/pfree.md)
  - [FreeAccessStrategy](../F/FreeAccessStrategy.md)
  - [UnregisterSnapshot](../U/UnregisterSnapshot.md)
- Data structures used:
  - [HeapScanDesc](../H/HeapScanDesc.md)
  - [TableScanDesc](../T/TableScanDesc.md)
- [Scan](../S/Scan.md) flags:
  - SO_TEMP_SNAPSHOT
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md)
  - HeapScanIsValid

## Notes and Other Information
- No locking manipulations are needed during scan termination
- The function releases both current buffer () and visibility map buffer () if they are valid
- Read stream must be freed before the BufferAccessStrategy to maintain proper resource dependency order
- The relation reference count is properly decremented to allow for relation cleanup when no longer in use
- Temporary snapshots (marked with  flag) are explicitly unregistered
- All dynamically allocated memory including scan keys, parallel worker data, and the scan descriptor itself is freed
- The function handles NULL pointers gracefully and only frees resources that were actually allocated

## Simplified Source

```c
void heap_endscan(TableScanDesc sscan) {
    HeapScanDesc scan = (HeapScanDesc) sscan;

    // Release scan buffers
    if (BufferIsValid(scan->rs_cbuf))
        ReleaseBuffer(scan->rs_cbuf);

    if (BufferIsValid(scan->rs_vmbuffer))
        ReleaseBuffer(scan->rs_vmbuffer);

    // End read stream before freeing access strategy
    if (scan->rs_read_stream)
        read_stream_end(scan->rs_read_stream);

    // Decrement relation reference count
    RelationDecrementReferenceCount(scan->rs_base.rs_rd);

    // Free scan keys
    if (scan->rs_base.rs_key)
        pfree(scan->rs_base.rs_key);

    // Free access strategy
    if (scan->rs_strategy != NULL)
        FreeAccessStrategy(scan->rs_strategy);

    // Free parallel worker data
    if (scan->rs_parallelworkerdata != NULL)
        pfree(scan->rs_parallelworkerdata);

    // Unregister temporary snapshot
    if (scan->rs_base.rs_flags & SO_TEMP_SNAPSHOT)
        UnregisterSnapshot(scan->rs_base.rs_snapshot);

    // Free scan descriptor
    pfree(scan);
}
```