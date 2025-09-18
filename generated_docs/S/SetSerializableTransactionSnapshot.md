# SetSerializableTransactionSnapshot

## Location
src/backend/storage/lmgr/predicate.c: 1712 - 1753

## Overview
Imports an externally-provided snapshot to be used for the current serializable transaction, setting up the serializable transaction context without taking a new snapshot.

## Definition
```c
void SetSerializableTransactionSnapshot(Snapshot snapshot, VirtualTransactionId *sourcevxid, int sourcepid)
```

## Detailed Description
This function is nearly identical to GetSerializableTransactionSnapshot, but instead of taking a new snapshot, it uses externally provided snapshot data. It's primarily used for importing snapshots from other serializable transactions, such as in parallel query execution or explicit snapshot sharing scenarios.

The function performs several important validations and handling:
1. Verifies that the current transaction is using serializable isolation
2. Handles parallel worker processes by returning early, since parallel workers will have their serializable context set up via AttachSerializableXact() instead
3. Explicitly prohibits SERIALIZABLE READ ONLY DEFERRABLE transactions from importing snapshots, since these transactions need to wait for safe snapshots which cannot be guaranteed with pre-determined imported snapshots
4. Delegates to GetSerializableTransactionSnapshotInt() with the provided source transaction information

The caller is responsible for ensuring that the imported snapshot comes from a serializable transaction and, if the current transaction is read-write, that the source transaction was not read-only.

## Parameters / Member Variables
- `snapshot`: The snapshot data structure to import and use for this transaction
- `sourcevxid`: Virtual transaction ID of the transaction that provided this snapshot
- `sourcepid`: Process ID of the source transaction

## Dependencies
- Functions called/Symbols referenced:
  - IsolationIsSerializable (verifies serializable isolation level)
  - IsParallelWorker (checks if running in parallel worker process)
  - GetSerializableTransactionSnapshotInt (main implementation for setting up serializable context)
  - VirtualTransactionId (type for virtual transaction identifier)
- Called from (representative examples):
  - SetTransactionSnapshot (in src/backend/utils/time/snapmgr.c:553)

## Notes and Other Information
- Cannot be used by SERIALIZABLE READ ONLY DEFERRABLE transactions - will throw an error since these transactions need to wait for safe snapshots
- Parallel workers return early without processing since their serializable context is managed by AttachSerializableXact()
- The caller must validate that the source snapshot is from a compatible serializable transaction
- Used primarily for snapshot sharing between processes and parallel query execution
- Part of PostgreSQL's Serializable Snapshot Isolation (SSI) implementation