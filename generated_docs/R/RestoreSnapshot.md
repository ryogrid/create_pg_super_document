# RestoreSnapshot

## Location
[src/backend/utils/time/snapmgr.c:1775-1839](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L1775-L1839)

## Overview
Deserializes a binary snapshot representation back into a PostgreSQL snapshot structure for use by parallel workers or other processes.

## Definition

```c
Snapshot
RestoreSnapshot(char *start_address)
```
## Detailed Description
RestoreSnapshot performs the inverse operation of SerializeSnapshot, reconstructing a full PostgreSQL snapshot structure from its serialized binary representation. This function is essential for parallel query execution, allowing worker processes to obtain the same MVCC snapshot that was active in the main process. The function allocates memory in TopTransactionContext and properly initializes all snapshot fields including transaction ID arrays, reference counts, and metadata flags.

The restored snapshot is marked as 'copied' to indicate it was reconstructed from serialized data rather than taken directly from the transaction manager. This affects how reference counting is handled by the snapshot management system.

## Parameters / Member Variables
- `*start_address`: Memory location containing the serialized snapshot data to be restored
## Dependencies
- Functions called/Symbols referenced:
  - [SerializedSnapshotData](../S/SerializedSnapshotData.md) (struct type for deserialization)
  - [SnapshotData](../S/SnapshotData.md) (target snapshot structure)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (for allocating snapshot memory)
  - SNAPSHOT_MVCC (snapshot type constant)
  - memcpy (for copying data from serialized format)
- Called from (representative examples):
  - [index_beginscan_parallel](../i/index_beginscan_parallel.md) (parallel index scanning initialization)
  - [table_beginscan_parallel](../t/table_beginscan_parallel.md) (parallel table scanning initialization)
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (parallel worker process startup)

## Notes and Other Information
- Memory is allocated in TopTransactionContext with initial reference counts set to 0
- The returned snapshot has the 'copied' flag set to true, indicating special handling for reference counting
- Handles both XID arrays and SubXID arrays when present in the serialized data
- Memory layout matches the format created by SerializeSnapshot: header followed by XID arrays
- Used exclusively in parallel query execution contexts where snapshot state must be shared between processes

## Simplified Source

```c
Snapshot
RestoreSnapshot(char *start_address)
{
    SerializedSnapshotData serialized_snapshot;
    Size size;
    Snapshot snapshot;
    TransactionId *serialized_xids;

    // Copy serialized header and locate XID arrays
    memcpy(&serialized_snapshot, start_address, sizeof(SerializedSnapshotData));
    serialized_xids = (TransactionId *) (start_address + sizeof(SerializedSnapshotData));

    // Calculate total size needed (header + XID arrays)
    size = sizeof(SnapshotData)
        + serialized_snapshot.xcnt * sizeof(TransactionId)
        + serialized_snapshot.subxcnt * sizeof(TransactionId);

    // Allocate and initialize snapshot structure
    snapshot = (Snapshot) MemoryContextAlloc(TopTransactionContext, size);
    snapshot->snapshot_type = SNAPSHOT_MVCC;
    snapshot->xmin = serialized_snapshot.xmin;
    snapshot->xmax = serialized_snapshot.xmax;
    snapshot->xcnt = serialized_snapshot.xcnt;
    snapshot->subxcnt = serialized_snapshot.subxcnt;
    snapshot->suboverflowed = serialized_snapshot.suboverflowed;
    snapshot->takenDuringRecovery = serialized_snapshot.takenDuringRecovery;
    snapshot->curcid = serialized_snapshot.curcid;
    snapshot->whenTaken = serialized_snapshot.whenTaken;
    snapshot->lsn = serialized_snapshot.lsn;
    snapshot->snapXactCompletionCount = 0;

    // Copy XID arrays if present
    if (serialized_snapshot.xcnt > 0) {
        snapshot->xip = (TransactionId *) (snapshot + 1);
        memcpy(snapshot->xip, serialized_xids,
               serialized_snapshot.xcnt * sizeof(TransactionId));
    }

    if (serialized_snapshot.subxcnt > 0) {
        snapshot->subxip = ((TransactionId *) (snapshot + 1)) + serialized_snapshot.xcnt;
        memcpy(snapshot->subxip, serialized_xids + serialized_snapshot.xcnt,
               serialized_snapshot.subxcnt * sizeof(TransactionId));
    }

    // Mark as copied snapshot with zero reference counts
    snapshot->regd_count = 0;
    snapshot->active_count = 0;
    snapshot->copied = true;

    return snapshot;
}
```