# ResourceOwnerDelete

## Location
[src/backend/utils/resowner/resowner.c:854-887](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/resowner/resowner.c#L854-L887)

## Overview
Safely deletes a resource owner and its entire descendant tree after ensuring all resources have been properly released.

## Definition

```c
void
ResourceOwnerDelete(ResourceOwner owner)
```
## Detailed Description
ResourceOwnerDelete is responsible for the complete destruction of a resource owner and its hierarchical descendants. This function implements a careful deletion strategy to maintain system integrity during the destruction process.

The function operates with strict preconditions: all resources must have been released before deletion can proceed. It validates that no resources remain in the owner's arrays, hash tables, or lock lists (except for the special overflow case where nlocks equals MAX_RESOWNER_LOCKS + 1).

The deletion process follows a specific order to prevent corruption:
1. Recursively deletes all child resource owners first
2. Unlinks the owner from its parent before destroying it
3. Frees the hash table storage if allocated
4. Deallocates the resource owner structure itself

This approach ensures that if an error occurs during deletion, the system maintains a consistent state rather than having dangling pointers or partial deletions that could cause crashes.

## Parameters / Member Variables
- : The ResourceOwner to delete along with all its descendants

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerDelete](ResourceOwnerDelete.md) (recursive call for child deletion)
  - [ResourceOwnerNewParent](ResourceOwnerNewParent.md) (to unlink from parent before deletion)
  - MAX_RESOWNER_LOCKS (constant for lock overflow detection)
  - [pfree](../p/pfree.md) (memory deallocation function)
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md) (transaction cleanup)
  - [PrepareTransaction](../P/PrepareTransaction.md) (prepared transaction cleanup)
  - [CleanupTransaction](../C/CleanupTransaction.md) (transaction error recovery)
  - [CommitSubTransaction](../C/CommitSubTransaction.md) (subtransaction cleanup)
  - PortalDrop (portal cleanup)
  - [WalSndResourceCleanup](../W/WalSndResourceCleanup.md) (WAL sender cleanup)

## Notes and Other Information
- Must not delete CurrentResourceOwner (enforced by assertion)
- Requires all resources to be released before deletion (verified by assertions)
- Uses recursive deletion to handle hierarchical resource owner trees
- Implements 'unlink before delete' strategy to prevent corruption on errors
- Handles both array-based and hash table-based resource storage cleanup
- Critical for proper transaction cleanup and memory management in PostgreSQL
- Used extensively in transaction processing, portal management, and cleanup operations