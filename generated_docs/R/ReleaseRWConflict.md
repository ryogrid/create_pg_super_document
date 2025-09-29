# ReleaseRWConflict

## Location
[src/backend/storage/lmgr/predicate.c:691-698](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L691-L698)

## Overview
Releases a read-write conflict record by removing it from transaction conflict lists and returning it to the available pool for reuse.

## Definition

```c
static void
ReleaseRWConflict(RWConflict conflict)
```
## Detailed Description
This function cleans up a read-write conflict record by removing it from both the inLink and outLink lists (which connect it to the involved transactions) and returning the conflict record to the RWConflictPool's available list for future reuse. This is part of the conflict management lifecycle in PostgreSQL's serializable snapshot isolation implementation.

## Parameters / Member Variables
- : Pointer to the RWConflict record to be released

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_delete](../d/dlist_delete.md)
  - [dlist_push_tail](../d/dlist_push_tail.md)
- Types referenced:
  - [RWConflict](RWConflict.md)
- Global variables accessed:
  - RWConflictPool
- Called from (representative examples):
  - [FlagSxactUnsafe](../F/FlagSxactUnsafe.md)
  - [ReleasePredicateLocks](ReleasePredicateLocks.md)
  - [ReleaseOneSerializableXact](ReleaseOneSerializableXact.md)

## Notes and Other Information
- Removes the conflict from both inLink and outLink lists to disconnect it from transactions
- Returns the conflict record to the pool for memory reuse
- Essential for proper cleanup during transaction completion or conflict resolution
- Part of PostgreSQL's resource management for serializable transactions
- Located in src/backend/storage/lmgr/predicate.c:691-698

## Simplified Source

```c
// Simplified version of ReleaseRWConflict
static void
ReleaseRWConflict(RWConflict conflict)
{
    // Remove conflict from both transaction link lists
    dlist_delete(&conflict->inLink);   // Remove from incoming transaction list
    dlist_delete(&conflict->outLink);  // Remove from outgoing transaction list

    // Return conflict record to available pool for reuse
    dlist_push_tail(&RWConflictPool->availableList, &conflict->outLink);
}
```

Key simplifications made:
- Added descriptive comments explaining each operation
- Clarified the purpose of each dlist operation (inLink vs outLink removal)
- Made the resource recycling pattern explicit with comments
- Preserved the complete original logic as the function is already quite simple