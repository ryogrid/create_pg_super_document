# HeapTupleSatisfiesVisibility

## Location
src/backend/access/heap/heapam_visibility.c: 1767 - 1788

## Overview
HeapTupleSatisfiesVisibility is a central dispatcher function that determines if a heap tuple satisfies visibility requirements according to different snapshot types in PostgreSQL's MVCC system.

## Definition
```c
bool HeapTupleSatisfiesVisibility(HeapTuple htup, Snapshot snapshot, Buffer buffer)
```

## Detailed Description
This function serves as the main entry point for tuple visibility checking in PostgreSQL's heap access method. It acts as a dispatcher that routes visibility checks to the appropriate specialized function based on the snapshot type. The function is designed to handle all different types of snapshots used throughout PostgreSQL, from regular MVCC snapshots to specialized snapshots for TOAST data, dirty reads, and historical queries.

The function assumes that the heap tuple is valid and that the buffer is at least share-locked. As a side effect of the visibility check, hint bits in the HeapTuple's t_infomask may be updated to cache visibility information, and if so, the buffer is marked dirty to ensure the changes are persisted.

## Parameters / Member Variables
- `htup`: The heap tuple to check for visibility
- `snapshot`: The snapshot context defining the visibility rules and transaction state
- `buffer`: The buffer containing the heap tuple, which must be at least share-locked

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleSatisfiesMVCC (for SNAPSHOT_MVCC)
  - HeapTupleSatisfiesSelf (for SNAPSHOT_SELF)  
  - HeapTupleSatisfiesAny (for SNAPSHOT_ANY)
  - HeapTupleSatisfiesToast (for SNAPSHOT_TOAST)
  - HeapTupleSatisfiesDirty (for SNAPSHOT_DIRTY)
  - HeapTupleSatisfiesHistoricMVCC (for SNAPSHOT_HISTORIC_MVCC)
  - HeapTupleSatisfiesNonVacuumable (for SNAPSHOT_NON_VACUUMABLE)
  - SNAPSHOT_* constants for snapshot type comparison
- Called from (representative examples):
  - page_collect_tuples (heap garbage collection)
  - heapgettup (sequential heap scan)
  - heap_fetch (tuple fetching)
  - heap_hot_search_buffer (HOT chain traversal)
  - heap_delete (tuple deletion)
  - heap_update (tuple update)
  - heapam_tuple_satisfies_snapshot (table AM interface)

## Notes and Other Information
This function is critical to PostgreSQL's MVCC implementation as it provides a unified interface for all visibility checks. The switch statement design allows for efficient dispatching to the appropriate visibility function while maintaining a clean separation of concerns for different snapshot types. The function may update hint bits as a performance optimization, caching visibility decisions in the tuple header to avoid repeated expensive visibility calculations.