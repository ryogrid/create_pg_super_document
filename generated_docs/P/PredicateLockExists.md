# PredicateLockExists

## Location
[src/backend/storage/lmgr/predicate.c:2035-2061](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L2035-L2061)

## Overview
A static function that checks whether a particular predicate lock is held by the current transaction using the local lock table.

## Definition

```c
static bool
PredicateLockExists(const PREDICATELOCKTARGETTAG *targettag)
```
## Detailed Description
This function verifies if a specific predicate lock target is currently held by the executing transaction. It operates by consulting the local predicate lock hash table (LocalPredicateLockHash) to find the lock entry and then checking its held status. 

The function has important limitations: it may return false positives or false negatives because the local lock table isn't always synchronized when other transactions modify lock lists (such as during index page splits). It can also return true when a coarser granularity lock that covers the target is being held. These characteristics make it suitable only for specific use cases where such inaccuracies are acceptable.

## Parameters / Member Variables
- `*targettag`: A pointer to the predicate lock target tag identifying the specific lock to check
## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md)
  - HASH_FIND (hash operation constant)
- Called from (representative examples):
  - [SerialControl](../S/SerialControl.md)
  - [CoarserLockCovers](../C/CoarserLockCovers.md)
  - [PredicateLockAcquire](PredicateLockAcquire.md)
  - [PredicateLockTID](PredicateLockTID.md)

## Notes and Other Information
- Static function, only accessible within predicate.c
- Uses local hash table which may not reflect real-time lock modifications by other transactions
- Returns false if no lock entry exists, true if lock exists and is held
- The function specifically checks the 'held' field of the LOCALPREDICATELOCK structure
- Should be used carefully due to potential false positives/negatives in concurrent scenarios
- Part of PostgreSQL's serializable snapshot isolation implementation

## Simplified Source

```c
static bool
PredicateLockExists(const PREDICATELOCKTARGETTAG *targettag)
{
    LOCALPREDICATELOCK *lock;

    // Look up lock in local hash table
    lock = hash_search(LocalPredicateLockHash, targettag, HASH_FIND, NULL);

    if (!lock)
        return false;

    // Found entry - check if it's actually held
    // (could be just a parent of some held lock)
    return lock->held;
}
```