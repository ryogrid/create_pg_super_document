# RegisterSnapshot

## Location
src/backend/utils/time/snapmgr.c: 794 - 806

## Overview
Registers a snapshot as being in use by the current resource owner, providing a convenient wrapper around RegisterSnapshotOnOwner.

## Definition
```c
Snapshot RegisterSnapshot(Snapshot snapshot)
```

## Detailed Description
RegisterSnapshot provides a simple interface for registering a snapshot with the current resource owner. It acts as a convenience wrapper that automatically uses CurrentResourceOwner. The function includes built-in null checking for InvalidSnapshot, making it safe to call with potentially invalid snapshots. When a valid snapshot is provided, it delegates to RegisterSnapshotOnOwner for the actual registration logic.

## Parameters / Member Variables
- `snapshot`: The snapshot to register. Can be InvalidSnapshot, which will be handled gracefully by returning InvalidSnapshot without registration.

## Dependencies
- Functions called/Symbols referenced:
  - RegisterSnapshotOnOwner
  - InvalidSnapshot
- Called from (representative examples):
  - _brin_begin_parallel
  - heapam_index_build_range_scan
  - systable_beginscan
  - table_beginscan_catalog
  - standard_ExecutorStart

## Notes and Other Information
- Returns InvalidSnapshot unchanged if passed InvalidSnapshot
- Always uses CurrentResourceOwner as the resource owner
- Part of PostgreSQL's snapshot management system for MVCC
- Provides a safer interface by handling null/invalid snapshots gracefully
- Located in src/backend/utils/time/snapmgr.c:794-806