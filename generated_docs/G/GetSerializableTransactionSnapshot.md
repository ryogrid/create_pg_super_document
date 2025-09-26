# GetSerializableTransactionSnapshot

## Location
[src/backend/storage/lmgr/predicate.c:1672-1711](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L1672-L1711)

## Overview
Acquires a snapshot that can be used for the current serializable transaction, ensuring proper setup of serializable transaction context and handling special optimizations for read-only deferrable transactions.

## Definition

```c
Snapshot
GetSerializableTransactionSnapshot(Snapshot snapshot)
```
## Detailed Description
This function is the main entry point for acquiring snapshots in serializable isolation level transactions. It ensures that the calling process has a proper SERIALIZABLEXACT reference in MySerializableXact and that it's contained in PredXact (the predicate lock manager's transaction table). 

The function performs several key validations and optimizations:
1. Verifies that the transaction is indeed using serializable isolation
2. Checks that the system is not in recovery mode (hot standby), as serializable isolation is not supported during recovery
3. Provides a special optimization path for SERIALIZABLE READ ONLY DEFERRABLE transactions by delegating to GetSafeSnapshot()
4. For regular serializable transactions, delegates to GetSerializableTransactionSnapshotInt()

The function maintains the same snapshot data structure passed in - no new allocation occurs within this function.

## Parameters / Member Variables
- : A pointer to a static Snapshot data area that can safely be passed to GetSnapshotData; this same pointer is returned

## Dependencies
- Functions called/Symbols referenced:
  - IsolationIsSerializable (checks if current isolation level is serializable)
  - [RecoveryInProgress](../R/RecoveryInProgress.md) (verifies system is not in recovery mode)
  - [GetSafeSnapshot](GetSafeSnapshot.md) (special path for read-only deferrable transactions)
  - [GetSerializableTransactionSnapshotInt](GetSerializableTransactionSnapshotInt.md) (main implementation for regular serializable transactions)
  - InvalidPid (constant used as parameter)
- Called from (representative examples):
  - [GetTransactionSnapshot](GetTransactionSnapshot.md) (in src/backend/utils/time/snapmgr.c:257)

## Notes and Other Information
- Only available when IsolationIsSerializable() returns true
- Cannot be used during hot standby recovery - will throw an error with guidance to use 'repeatable read' instead
- For SERIALIZABLE READ ONLY DEFERRABLE transactions, provides an optimization by using GetSafeSnapshot() which can wait for a suitable snapshot to avoid SSI (Serializable Snapshot Isolation) overhead
- The function is part of PostgreSQL's predicate locking system for implementing true serializable isolation