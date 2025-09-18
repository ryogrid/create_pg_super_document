# heap_endscan

## Location
src/backend/access/heap/heapam.c: 1254 - 1295

## Overview
Terminates a heap table scan by releasing all associated resources including buffers, access strategies, and scan descriptors.

## Definition


## Detailed Description
The  function performs cleanup operations to terminate a heap table scan and free all associated resources. This includes releasing any pinned buffers, ending read streams, decrementing relation reference counts, freeing memory allocations, and cleaning up temporary snapshots. The function ensures proper resource management by releasing resources in the correct order - notably freeing the read stream before the BufferAccessStrategy to avoid dependency issues.

## Parameters / Member Variables
- : The table scan descriptor to terminate (cast to HeapScanDesc internally)

## Dependencies
- Functions called/Symbols referenced:
  - ReleaseBuffer
  - read_stream_end
  - [RelationDecrementReferenceCount](../R/RelationDecrementReferenceCount.md)
  - [pfree](../p/pfree.md)
  - FreeAccessStrategy
  - UnregisterSnapshot
- Data structures used:
  - [HeapScanDesc](../H/HeapScanDesc.md)
  - [TableScanDesc](../T/TableScanDesc.md)
- Scan flags:
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