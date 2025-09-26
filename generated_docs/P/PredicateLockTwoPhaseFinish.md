# PredicateLockTwoPhaseFinish

## Location
[src/backend/storage/lmgr/predicate.c:4872-4898](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L4872-L4898)

## Overview
Releases predicate locks and cleans up serializable transaction state when a prepared transaction either commits or aborts in two-phase commit.

## Definition
```c
void PredicateLockTwoPhaseFinish(TransactionId xid, bool isCommit)
```

## Detailed Description
This function completes the two-phase commit process for serializable transactions by releasing all predicate locks held by a prepared transaction. It is called when a prepared transaction is either committed or aborted to ensure proper cleanup of the serializable transaction state and associated predicate locks.

The function first looks up the prepared transaction by its transaction ID in the SerializableXidHash to find the corresponding SERIALIZABLEXACT structure. If found, it temporarily sets the global MySerializableXact context to point to the prepared transaction's state, then calls ReleasePredicateLocks to perform the actual lock cleanup and conflict resolution.

The function conservatively assumes that the prepared transaction performed writes (sets MyXactDidWrite = true) to ensure proper serialization conflict handling during cleanup. This is necessary because the actual write status may not be accurately preserved across the prepare/finish phases.

## Parameters / Member Variables
- `xid`: The transaction ID of the prepared transaction being finished
- `isCommit`: Boolean indicating whether the transaction is being committed (true) or aborted (false)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md): Searches SerializableXidHash for the transaction
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease: Manages SerializableXactHashLock for safe hash access
  - [ReleasePredicateLocks](../R/ReleasePredicateLocks.md): Releases all predicate locks and handles conflicts
  - HASH_FIND: Hash search operation type for lookup
  - LW_SHARED: Lock mode for shared access to hash table
- Called from (representative examples):
  - [FinishPreparedTransaction](../F/FinishPreparedTransaction.md): During completion of prepared transactions

## Notes and Other Information
- Returns early if the transaction ID is not found in SerializableXidHash (not a serializable transaction)
- Uses shared lock on SerializableXactHashLock since it only needs to read the hash table
- Conservatively sets MyXactDidWrite = true to ensure proper conflict handling regardless of actual write status
- The temporary setting of MySerializableXact allows ReleasePredicateLocks to operate on the correct transaction context
- This function handles both commit and abort cases - [ReleasePredicateLocks](../R/ReleasePredicateLocks.md) handles the different behaviors based on the isCommit parameter
- Critical for maintaining serializable isolation guarantees across two-phase commit boundaries