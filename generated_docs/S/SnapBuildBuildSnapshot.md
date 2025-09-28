# SnapBuildBuildSnapshot

## Location
[src/backend/replication/logical/snapbuild.c:499-578](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L499-L578)

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
  - [SnapBuildCommitTxn](SnapBuildCommitTxn.md)
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

## Simplified Source

```c
// Simplified version of SnapBuildBuildSnapshot
static Snapshot SnapBuildBuildSnapshot(SnapBuild *builder) {
    Snapshot snapshot;
    Size ssize;

    // Ensure we're in the correct state for snapshot building
    Assert(builder->state >= SNAPBUILD_FULL_SNAPSHOT);

    // Calculate memory needed for snapshot and transaction arrays
    ssize = sizeof(SnapshotData) +
            sizeof(TransactionId) * builder->committed.xcnt +
            sizeof(TransactionId) * 1; /* toplevel xid */

    // Allocate and initialize snapshot structure
    snapshot = MemoryContextAllocZero(builder->context, ssize);
    snapshot->snapshot_type = SNAPSHOT_HISTORIC_MVCC;

    // Set transaction range boundaries
    snapshot->xmin = builder->xmin;
    snapshot->xmax = builder->xmax;

    // Set up array of committed transactions to be treated as visible
    snapshot->xip = (TransactionId *) ((char *) snapshot + sizeof(SnapshotData));
    snapshot->xcnt = builder->committed.xcnt;

    // Copy committed transaction IDs
    memcpy(snapshot->xip, builder->committed.xip,
           builder->committed.xcnt * sizeof(TransactionId));

    // Sort transaction array for efficient binary search
    qsort(snapshot->xip, snapshot->xcnt, sizeof(TransactionId), xidComparator);

    // Initialize subtransaction fields (empty initially)
    snapshot->subxcnt = 0;
    snapshot->subxip = NULL;
    snapshot->suboverflowed = false;

    // Set standard snapshot properties
    snapshot->takenDuringRecovery = false;
    snapshot->copied = false;
    snapshot->curcid = FirstCommandId;
    snapshot->active_count = 0;
    snapshot->regd_count = 0;
    snapshot->snapXactCompletionCount = 0;

    return snapshot;
}
```

Key simplifications made:
- Removed detailed comments about xip/subxip field repurposing
- Consolidated memory layout calculations
- Simplified the transaction ID validation logic
- Focused on the core snapshot construction steps
- Abstracted complex field initialization into logical groups
- Preserved the essential sorting and memory management logic