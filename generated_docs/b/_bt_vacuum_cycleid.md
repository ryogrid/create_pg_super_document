# _bt_vacuum_cycleid

## Location
src/backend/access/nbtree/nbtutils.c: 4394 - 4427

## Overview
Retrieves the active vacuum cycle ID for a B-tree index, returning zero if no vacuum operation is currently active on the index.

## Definition


## Detailed Description
This function provides coordination between B-tree maintenance operations and active VACUUM processes by returning the current vacuum cycle ID for a given index relation. It searches through the global btvacinfo structure under BtreeVacuumLock to find any active vacuum operation on the specified relation. The function is primarily used during page splits to ensure proper interlocking with concurrent vacuum operations. When a vacuum cycle ID is stored in newly created pages during splits, it prevents the vacuum from processing those pages until the split operation is complete, maintaining index consistency.

## Parameters / Member Variables
- : Relation descriptor for the B-tree index being queried

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire/LWLockRelease
  - BtreeVacuumLock (LWLock)
  - btvacinfo (global vacuum info structure)
  - [BTOneVacInfo](../B/BTOneVacInfo.md) (vacuum info structure)
  - BTCycleId (cycle identifier type)
- Called from (representative examples):
  - [_bt_split](_bt_split.md)

## Notes and Other Information
- Uses shared lock on BtreeVacuumLock since this is a read-only operation
- Critical for preventing race conditions between page splits and vacuum operations
- Returns zero when no active vacuum is found for the relation
- Caller must hold exclusive lock on buffers where cycle ID will be stored
- Part of PostgreSQL's B-tree concurrency control mechanism
- Located in src/backend/access/nbtree/nbtutils.c:4394-4427