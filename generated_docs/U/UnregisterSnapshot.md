# UnregisterSnapshot

## Location
[src/backend/utils/time/snapmgr.c:836-848](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L836-L848)

## Overview
Decrements the reference count of a snapshot and removes the corresponding reference from CurrentResourceOwner, providing a convenient wrapper around UnregisterSnapshotFromOwner.

## Definition
```c
void UnregisterSnapshot(Snapshot snapshot)
```

## Detailed Description
UnregisterSnapshot provides a simple interface for unregistering a snapshot from the current resource owner. It acts as a convenience wrapper that automatically uses CurrentResourceOwner and delegates to UnregisterSnapshotFromOwner for the actual unregistration logic. The function includes built-in null checking, making it safe to call with NULL snapshots. This function is part of the snapshot lifecycle management system that ensures proper cleanup and reference counting.

## Parameters / Member Variables
- `snapshot`: The snapshot to unregister. Can be NULL, which will be handled gracefully by returning without action.

## Dependencies
- Functions called/Symbols referenced:
  - UnregisterSnapshotFromOwner
- Called from (representative examples):
  - _brin_begin_parallel
  - heap_endscan
  - systable_endscan
  - index_endscan
  - standard_ExecutorEnd
  - FreeQueryDesc

## Notes and Other Information
- Returns immediately without action if passed NULL snapshot
- Always uses CurrentResourceOwner as the resource owner
- Part of PostgreSQL's snapshot management system for MVCC
- Provides a safer interface by handling null snapshots gracefully
- Counterpart to RegisterSnapshot for snapshot lifecycle management
- Located in src/backend/utils/time/snapmgr.c:836-848