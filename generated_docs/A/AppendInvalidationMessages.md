# AppendInvalidationMessages

## Location
src/backend/utils/cache/inval.c: 501 - 514

## Overview
Appends all invalidation messages from a source group to a destination group, resetting the source group to empty.

## Definition
```c
static void AppendInvalidationMessages(InvalidationMsgsGroup *dest, InvalidationMsgsGroup *src)
```

## Detailed Description
This function transfers all invalidation messages from the source InvalidationMsgsGroup to the destination InvalidationMsgsGroup. It operates on both message subgroups (CatCacheMsgs and RelCacheMsgs) by calling AppendInvalidationMessageSubGroup for each. After the operation, the source group is left empty and ready for reuse, while the destination group contains the combined messages from both groups.

This function is typically used when consolidating invalidation messages from different contexts, such as moving messages from transaction-local storage to session-level storage during transaction commit or abort processing.

## Parameters / Member Variables
- `dest`: Pointer to the destination InvalidationMsgsGroup that will receive the appended messages
- `src`: Pointer to the source InvalidationMsgsGroup whose messages will be moved to the destination

## Dependencies
- Functions called/Symbols referenced:
  - AppendInvalidationMessageSubGroup (performs the actual message transfer for each subgroup)
  - CatCacheMsgs (catalog cache message subgroup identifier)
  - RelCacheMsgs (relation cache message subgroup identifier)
- Types referenced:
  - InvalidationMsgsGroup
- Called from:
  - AtEOXact_Inval (at transaction end)
  - AtEOSubXact_Inval (at subtransaction end)
  - CommandEndInvalidationMessages (at command completion)

## Notes and Other Information
- This is a static function, only accessible within the inval.c module
- The function assumes that messages in the destination and source groups are adjacent in the underlying message array
- After the operation, the source group is reset to follow the destination group, preventing dangling references
- Part of PostgreSQL's invalidation message management system for maintaining cache coherency
- Used primarily during transaction state transitions to consolidate messages across different scopes