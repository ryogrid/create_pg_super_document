# SnapBuildBuildSnapshot

## Location
src/backend/replication/logical/snapbuild.c: 499 - 578

## Overview
Creates a new historic MVCC snapshot based on currently committed catalog-modifying transactions, providing the foundation for consistent logical decoding operations.

## Definition
```c
static Snapshot SnapBuildBuildSnapshot(SnapBuild *builder)
```

## Detailed Description
This static function is the core snapshot creation mechanism in PostgreSQL's logical replication system. It constructs a specialized SNAPSHOT_HISTORIC_MVCC snapshot that tracks catalog-modifying transactions for consistent logical decoding. The function allocates memory for the snapshot structure plus arrays to hold transaction IDs, then populates the snapshot with the current transaction range (xmin/xmax) and the list of committed transactions that should be treated as visible. The snapshot uses a specialized interpretation of the standard PostgreSQL snapshot fields: the 'xip' array stores transactions to be treated as committed, and 'subxip' is initially empty but can be filled later for transactions that modify the catalog. Both arrays are sorted to enable efficient binary searches during visibility checks.

## Parameters / Member Variables
- `builder`: Pointer to the SnapBuild structure containing the current snapshot building state

## Dependencies
- Functions called/Symbols referenced:
  - [SnapBuild](SnapBuild.md)
  - SNAPBUILD_FULL_SNAPSHOT
  - [SnapshotData](SnapshotData.md)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - SNAPSHOT_HISTORIC_MVCC
  - TransactionIdIsNormal
  - [xidComparator](../x/xidComparator.md)
  - qsort
  - FirstCommandId
- Called from (representative examples):
  - [SnapBuildInitialSnapshot](SnapBuildInitialSnapshot.md)
  - [SnapBuildGetOrBuildSnapshot](SnapBuildGetOrBuildSnapshot.md)
  - [SnapBuildProcessChange](SnapBuildProcessChange.md)
  - SnapBuildCommitTxn
  - [SnapBuildRestore](SnapBuildRestore.md)

## Notes and Other Information
- Static function only accessible within snapbuild.c
- Requires builder to be in SNAPBUILD_FULL_SNAPSHOT state or later
- Creates snapshots optimized for logical decoding rather than regular transaction visibility
- Uses specialized interpretation of snapshot fields for efficiency in catalog change tracking
- Sorts transaction arrays to enable binary search during visibility checks
- Allocates snapshot in the builder's memory context for proper lifecycle management
- Initially creates snapshots without subtransaction information (subxip empty)
- Critical component of the logical replication consistency mechanism
- Located in src/backend/replication/logical/snapbuild.c:499-578