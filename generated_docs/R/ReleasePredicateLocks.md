# ReleasePredicateLocks

## Location
[src/backend/storage/lmgr/predicate.c:3302-3668](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L3302-L3668)

## Overview
Releases predicate locks and manages cleanup when a serializable transaction commits, rolls back, or becomes read-only safe in PostgreSQL's serializable snapshot isolation system.

## Definition
```c
void ReleasePredicateLocks(bool isCommit, bool isReadOnlySafe)
```

## Detailed Description
ReleasePredicateLocks is a comprehensive function that handles the complex task of cleaning up predicate locks and related data structures when a serializable transaction completes or becomes safe to release early. This function is central to maintaining the correctness and performance of PostgreSQL's serializable snapshot isolation implementation.

The function handles several key scenarios:

1. **Transaction Completion**: When transactions commit or rollback, it properly releases locks and updates conflict tracking structures
2. **Early Release for RO_SAFE**: Read-only transactions determined to be safe can release locks before transaction end
3. **Parallel Query Support**: Special handling for parallel workers and leaders, including partial release mechanics
4. **Conflict Management**: Updates read-write conflict tracking between transactions
5. **Global State Maintenance**: Updates global minimum xmin values and triggers cleanup of old transactions
6. **DEFERRABLE Transaction Coordination**: Wakes up waiting deferrable transactions when their safety status becomes known

The function performs extensive cleanup of shared memory hash tables, manages the transition of transactions to finished states, and coordinates with PostgreSQL's parallel query infrastructure. It ensures that serialization conflict detection remains accurate while allowing timely cleanup of resources.

## Parameters / Member Variables
- `isCommit`: True if the transaction is committing, false for rollback or early release
- `isReadOnlySafe`: True if releasing early due to read-only safety determination (cannot be true with isCommit)

## Dependencies
- Functions called/Symbols referenced:
  - IsParallelWorker/ParallelContextActive (parallel query coordination)
  - [ReleasePredicateLocksLocal](ReleasePredicateLocksLocal.md) (local backend cleanup)
  - SxactIsPartiallyReleased/SxactIsCommitted/SxactIsReadOnly (transaction state checks)
  - [ReleaseRWConflict](ReleaseRWConflict.md) (conflict cleanup)
  - [SetNewSxactGlobalXmin](../S/SetNewSxactGlobalXmin.md) (global xmin management)
  - [ClearOldPredicateLocks](../C/ClearOldPredicateLocks.md) (old transaction cleanup)
  - [FlagSxactUnsafe](../F/FlagSxactUnsafe.md) (read-only transaction conflict marking)
  - [ProcSendSignal](../P/ProcSendSignal.md) (deferrable transaction wakeup)
  - [ReleaseOneSerializableXact](ReleaseOneSerializableXact.md) (transaction structure cleanup)
- Called from (representative examples):
  - [SerializationNeededForRead](../S/SerializationNeededForRead.md) (early release for safe read-only transactions)
  - [GetSafeSnapshot](../G/GetSafeSnapshot.md) (snapshot acquisition safety checks)
  - [PredicateLockTwoPhaseFinish](../P/PredicateLockTwoPhaseFinish.md) (two-phase commit completion)
  - [ResourceOwnerReleaseInternal](ResourceOwnerReleaseInternal.md) (resource cleanup)

## Notes and Other Information
- Must handle complex parallel query scenarios where workers and leaders have different responsibilities
- Performs extensive assertion checking to ensure data structure consistency
- Supports both complete and partial release of transaction structures for parallel safety
- Updates global counters that affect system-wide cleanup decisions
- Handles the complex interaction between read-write conflicts and transaction commit ordering
- Essential for preventing memory leaks in long-running systems with many serializable transactions
- The function's logic must be carefully coordinated with vacuum and other cleanup processes
- Supports PostgreSQL's DEFERRABLE transaction feature by managing wait/wake semantics