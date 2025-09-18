# HeapTupleSatisfiesVisibility

## Location
[src/backend/access/heap/heapam_visibility.c:1767-1788](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_visibility.c#L1767-L1788)

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
  - [HeapTupleSatisfiesMVCC](HeapTupleSatisfiesMVCC.md) (for SNAPSHOT_MVCC)
  - [HeapTupleSatisfiesSelf](HeapTupleSatisfiesSelf.md) (for SNAPSHOT_SELF)  
  - [HeapTupleSatisfiesAny](HeapTupleSatisfiesAny.md) (for SNAPSHOT_ANY)
  - [HeapTupleSatisfiesToast](HeapTupleSatisfiesToast.md) (for SNAPSHOT_TOAST)
  - [HeapTupleSatisfiesDirty](HeapTupleSatisfiesDirty.md) (for SNAPSHOT_DIRTY)
  - [HeapTupleSatisfiesHistoricMVCC](HeapTupleSatisfiesHistoricMVCC.md) (for SNAPSHOT_HISTORIC_MVCC)
  - [HeapTupleSatisfiesNonVacuumable](HeapTupleSatisfiesNonVacuumable.md) (for SNAPSHOT_NON_VACUUMABLE)
  - SNAPSHOT_* constants for snapshot type comparison
- Called from (representative examples):
  - [page_collect_tuples](../p/page_collect_tuples.md) (heap garbage collection)
  - [heapgettup](../h/heapgettup.md) (sequential heap scan)
  - [heap_fetch](../h/heap_fetch.md) (tuple fetching)
  - [heap_hot_search_buffer](../h/heap_hot_search_buffer.md) (HOT chain traversal)
  - [heap_delete](../h/heap_delete.md) (tuple deletion)
  - [heap_update](../h/heap_update.md) (tuple update)
  - [heapam_tuple_satisfies_snapshot](../h/heapam_tuple_satisfies_snapshot.md) (table AM interface)

## Notes and Other Information
This function is critical to PostgreSQL's MVCC implementation as it provides a unified interface for all visibility checks. The switch statement design allows for efficient dispatching to the appropriate visibility function while maintaining a clean separation of concerns for different snapshot types. The function may update hint bits as a performance optimization, caching visibility decisions in the tuple header to avoid repeated expensive visibility calculations.