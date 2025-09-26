# PageIsPredicateLocked

## Location
[src/backend/storage/lmgr/predicate.c:1998-2034](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L1998-L2034)

## Overview
Checks whether there are any predicate locks held by any transaction for a specific page in a PostgreSQL relation, used primarily for serializable snapshot isolation support.

## Definition

```c
bool
PageIsPredicateLocked(Relation relation, BlockNumber blkno)
```
## Detailed Description
This function determines if any predicate locks exist on a specific page of a relation. It's a key component of PostgreSQL's serializable snapshot isolation implementation. The function searches the predicate lock target hash table for locks specifically targeting the given page, without considering broader relation-level locks. It handles cases where transactions may be completed but not yet cleaned up due to overlapping serializable transactions, ensuring reliable information regardless of the current transaction's isolation level.

The function is particularly important for GiST index vacuum operations, where it's necessary to know if a page is predicate-locked before performing vacuum operations that could affect serializable transaction consistency.

## Parameters / Member Variables
- : The relation containing the page to check for predicate locks
- : The block number of the specific page within the relation to check

## Dependencies
- Functions called/Symbols referenced:
  - SET_PREDICATELOCKTARGETTAG_PAGE
  - PredicateLockTargetTagHashCode
  - PredicateLockHashPartitionLock
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - [LWLockRelease](../L/LWLockRelease.md)
- Called from (representative examples):
  - SerializableXactHandle (referenced in predicate.h)

## Notes and Other Information
- Returns true if any predicate lock exists on the specified page, false otherwise
- Uses lightweight locking (LW_SHARED) to safely access the predicate lock hash table
- Specifically checks page-level locks, not relation-level locks
- Essential for maintaining consistency during concurrent GiST index operations
- Part of PostgreSQL's implementation of true serializable isolation level