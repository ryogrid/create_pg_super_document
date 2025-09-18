# SnapBuildFreeSnapshot

## Location
[src/backend/replication/logical/snapbuild.c:391-415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L391-L415)

## Overview
SnapBuildFreeSnapshot safely deallocates an unreferenced snapshot that was previously built by the snapshot builder, with extensive validation to ensure snapshot integrity.

## Definition
```c
static void SnapBuildFreeSnapshot(Snapshot snap)
```

## Detailed Description
This static function is responsible for freeing snapshots that were created by the snapshot building mechanism. It performs comprehensive validation to ensure that only properly managed snapshots are freed. The function checks that the snapshot is of the correct type (SNAPSHOT_HISTORIC_MVCC), hasn't been modified inappropriately, and isn't currently in use. It includes both debug assertions and runtime error checks to prevent corruption or misuse of the snapshot system. Only snapshots that are no longer referenced and not currently active can be safely freed.

## Parameters / Member Variables
- `snap`: Pointer to the Snapshot structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  - Assert (multiple validation checks)
  - elog
  - [pfree](../p/pfree.md)
  - SNAPSHOT_HISTORIC_MVCC
  - FirstCommandId
- Called from (representative examples):
  - [SnapBuildSnapDecRefcount](SnapBuildSnapDecRefcount.md)
  - [SnapBuild](SnapBuild.md) structure's freefunc field

## Notes and Other Information
The function enforces strict invariants about snapshot state: the snapshot must be of type SNAPSHOT_HISTORIC_MVCC, have curcid set to FirstCommandId, not be suboverflowed or taken during recovery, have zero registered count, not be copied, and not be active. These checks ensure that only snapshots created and managed by the logical replication snapshot building system are freed through this function, preventing accidental freeing of external or system snapshots. The function uses both compile-time assertions (Assert) for debugging and runtime checks (elog) for critical validation.