# LockHasWaitersRelation

## Location
src/backend/storage/lmgr/lmgr.c: 363 - 386

## Overview
LockHasWaitersRelation checks whether other transactions are waiting for a specific lock mode on a relation that the current transaction is holding.

## Definition
```c
bool LockHasWaitersRelation(Relation relation, LOCKMODE lockmode)
```

## Detailed Description
This function determines if there are other processes waiting to acquire a lock on the specified relation with the given lock mode. It constructs a lock tag from the relation's database and relation identifiers, then delegates to LockHasWaiters to check for waiting processes. This is useful for making decisions about lock release timing or conflict detection.

## Parameters / Member Variables
- `relation`: The relation to check for lock waiters
- `lockmode`: The lock mode to check for waiting processes

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_RELATION (macro to construct relation lock tag)
  - LockHasWaiters (performs the actual waiter check)
- Called from (representative examples):
  - count_nondeletable_pages (in vacuum operations)

## Notes and Other Information
- Returns true if there are processes waiting for the specified lock on the relation
- Uses false as the third parameter to LockHasWaiters, indicating exact lock mode matching
- Commonly used in vacuum and maintenance operations to check for contention
- Located in src/backend/storage/lmgr/lmgr.c:363-386