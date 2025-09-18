# smgrpin

## Location
src/backend/storage/smgr/smgr.c: 250 - 264

## Overview
Prevents an SMgrRelation object from being destroyed at the end of a transaction by incrementing its reference count.

## Definition
```c
void smgrpin(SMgrRelation reln)
```

## Detailed Description
The `smgrpin` function implements a reference counting mechanism to control the lifetime of SMgrRelation objects. When called, it increments the pin count of the specified relation object, preventing it from being automatically destroyed during transaction cleanup. If the relation was previously unpinned (pincount == 0), it removes the relation from the unpinned_relns list since it no longer needs to be tracked for automatic cleanup. This function is essential for code that needs to hold references to storage manager relation objects beyond the current transaction boundary.

## Parameters / Member Variables
- `reln`: SMgrRelation object to be pinned

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_delete](../d/dlist_delete.md)
  - SMgrRelation (type)
- Called from (representative examples):
  - RelationGetSmgr (src/include/utils/rel.h:572)

## Notes and Other Information
- This function uses reference counting to manage object lifetime
- When pincount transitions from 0 to 1, the relation is removed from the unpinned_relns list
- Pinned relations must be explicitly unpinned using smgrunpin to allow cleanup
- The pinning mechanism allows relation objects to survive transaction boundaries
- Multiple pins on the same relation are supported (pincount can exceed 1)