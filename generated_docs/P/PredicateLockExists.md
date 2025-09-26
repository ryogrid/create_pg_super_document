# PredicateLockExists

## Location
src/backend/storage/lmgr/predicate.c: 2035 - 2061

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
- : A pointer to the predicate lock target tag identifying the specific lock to check

## Dependencies
- Functions called/Symbols referenced:
  - hash_search
  - HASH_FIND (hash operation constant)
- Called from (representative examples):
  - SerialControl
  - CoarserLockCovers
  - PredicateLockAcquire
  - PredicateLockTID

## Notes and Other Information
- Static function, only accessible within predicate.c
- Uses local hash table which may not reflect real-time lock modifications by other transactions
- Returns false if no lock entry exists, true if lock exists and is held
- The function specifically checks the 'held' field of the LOCALPREDICATELOCK structure
- Should be used carefully due to potential false positives/negatives in concurrent scenarios
- Part of PostgreSQL's serializable snapshot isolation implementation