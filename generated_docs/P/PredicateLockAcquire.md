# PredicateLockAcquire

## Location
[src/backend/storage/lmgr/predicate.c:2507-2565](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L2507-L2565)

## Overview
Acquires a predicate lock on a specified target for the current connection, handling granularity promotion and local lock table management.

## Definition
static void PredicateLockAcquire(const PREDICATELOCKTARGETTAG *targettag)

## Detailed Description
This function is the main entry point for acquiring predicate locks in PostgreSQL's serializable snapshot isolation implementation. It implements a sophisticated locking strategy that includes checking for existing locks, evaluating lock coverage by coarser granularity locks, and managing both local and shared lock tables.

The function first checks if the lock already exists or is covered by a coarser lock, returning early if so. If a new lock is needed, it updates the local lock table and calls CreatePredicateLock to establish the shared lock. After acquisition, it attempts lock promotion to coarser granularity and cleans up any finer-granularity locks that are now redundant. This approach optimizes lock granularity to balance concurrency with lock table space usage.

## Parameters / Member Variables
- : Pointer to the predicate lock target tag identifying the specific resource to be locked

## Dependencies
- Functions called/Symbols referenced:
  - [PredicateLockExists](PredicateLockExists.md)
  - [CoarserLockCovers](../C/CoarserLockCovers.md)
  - PredicateLockTargetTagHashCode
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - [CreatePredicateLock](../C/CreatePredicateLock.md)
  - [CheckAndPromotePredicateLockRequest](../C/CheckAndPromotePredicateLockRequest.md)
  - [DeleteChildTargetLocks](../D/DeleteChildTargetLocks.md)
  - GET_PREDICATELOCKTARGETTAG_TYPE
  - [LOCALPREDICATELOCK](../L/LOCALPREDICATELOCK.md) (struct)
  - PREDLOCKTAG_TUPLE (constant)
  - MySerializableXact (global variable)
- Called from (representative examples):
  - [PredicateLockRelation](PredicateLockRelation.md)
  - [PredicateLockPage](PredicateLockPage.md)
  - [PredicateLockTID](PredicateLockTID.md)
  - [CheckAndPromotePredicateLockRequest](../C/CheckAndPromotePredicateLockRequest.md)
  - [SerialControl](../S/SerialControl.md)

## Notes and Other Information
- Central function in PostgreSQL's predicate locking mechanism for serializable transactions
- Implements granularity promotion where multiple fine-grained locks can be consolidated into coarser ones
- Uses both local and shared lock tables: local for quick checks and shared for cross-transaction visibility
- Automatically cleans up redundant finer-granularity locks except for tuple-level locks
- The function is idempotent - calling it multiple times with the same target has no additional effect
- Critical for preventing serialization anomalies in SERIALIZABLE isolation level transactions

## Simplified Source

```c
static void
PredicateLockAcquire(const PREDICATELOCKTARGETTAG *targettag)
{
    uint32 targettaghash;
    bool found;
    LOCALPREDICATELOCK *locallock;

    // Check if we already have this lock or a covering coarser lock
    if (PredicateLockExists(targettag))
        return;

    if (CoarserLockCovers(targettag))
        return;

    // Calculate hash for both local and shared lock tables
    targettaghash = PredicateLockTargetTagHashCode(targettag);

    // Add entry to local lock table
    locallock = hash_search_with_hash_value(LocalPredicateLockHash,
                                           targettag, targettaghash,
                                           HASH_ENTER, &found);
    locallock->held = true;
    if (!found)
        locallock->childLocks = 0;

    // Create the actual predicate lock
    CreatePredicateLock(targettag, targettaghash, MySerializableXact);

    // Try to promote to coarser granularity or clean up finer locks
    if (CheckAndPromotePredicateLockRequest(targettag))
    {
        // Lock was promoted - promotion logic handles cleanup
    }
    else
    {
        // Clean up any finer-granularity child locks (except tuples)
        if (GET_PREDICATELOCKTARGETTAG_TYPE(*targettag) != PREDLOCKTAG_TUPLE)
            DeleteChildTargetLocks(targettag);
    }
}
```