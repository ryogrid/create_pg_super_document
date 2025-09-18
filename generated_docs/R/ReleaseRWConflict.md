# ReleaseRWConflict

## Location
src/backend/storage/lmgr/predicate.c: 691 - 698

## Overview
Releases a read-write conflict record by removing it from transaction conflict lists and returning it to the available pool for reuse.

## Definition


## Detailed Description
This function cleans up a read-write conflict record by removing it from both the inLink and outLink lists (which connect it to the involved transactions) and returning the conflict record to the RWConflictPool's available list for future reuse. This is part of the conflict management lifecycle in PostgreSQL's serializable snapshot isolation implementation.

## Parameters / Member Variables
- : Pointer to the RWConflict record to be released

## Dependencies
- Functions called/Symbols referenced:
  - dlist_delete
  - dlist_push_tail
- Types referenced:
  - RWConflict
- Global variables accessed:
  - RWConflictPool
- Called from (representative examples):
  - FlagSxactUnsafe
  - ReleasePredicateLocks
  - ReleaseOneSerializableXact

## Notes and Other Information
- Removes the conflict from both inLink and outLink lists to disconnect it from transactions
- Returns the conflict record to the pool for memory reuse
- Essential for proper cleanup during transaction completion or conflict resolution
- Part of PostgreSQL's resource management for serializable transactions
- Located in src/backend/storage/lmgr/predicate.c:691-698