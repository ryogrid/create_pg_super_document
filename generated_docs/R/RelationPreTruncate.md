# RelationPreTruncate

## Location
[src/backend/catalog/storage.c:449-476](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/storage.c#L449-L476)

## Overview
RelationPreTruncate performs access method-independent preparatory work before a physical relation truncation, specifically managing pending sync operations for relations that are about to be truncated.

## Definition
```c
void RelationPreTruncate(Relation rel)
```

## Detailed Description
RelationPreTruncate is a helper function that must be called before any physical truncation of a relation. Its primary purpose is to mark relations in the pending sync hash as truncated, which is crucial for proper synchronization management.

The function operates by:
1. Checking if there is an active pendingSyncHash (returns early if none exists)
2. Searching for the relation's locator in the pending sync hash
3. If found, marking the pending sync entry as truncated by setting `is_truncated = true`

This is particularly important for access methods that implement custom truncation logic through `relation_nontransactional_truncate` but don't call the standard `RelationTruncate()` function. Such access methods must call this function to ensure proper coordination with the sync management system.

## Parameters / Member Variables
- `rel`: The Relation that is about to be truncated

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md)
  - [RelationGetSmgr](RelationGetSmgr.md)
  - [PendingRelSync](../P/PendingRelSync.md) (struct type)
  - HASH_FIND (constant)
- Called from (representative examples):
  - [RelationTruncate](RelationTruncate.md)

## Notes and Other Information
- This function is lightweight and safe to call even when no pending syncs exist
- Essential for access methods that implement custom truncation logic
- Works with the pending sync system to track truncated relations
- Must be called before any reduction in table size occurs
- The function is designed to be access method-independent, providing a common interface for truncation preparation

## Simplified Source

```c
void
RelationPreTruncate(Relation rel)
{
    PendingRelSync *pending;

    // Exit early if no pending sync hash exists
    if (!pendingSyncHash)
        return;

    // Look for this relation in the pending sync hash
    pending = hash_search(pendingSyncHash,
                         &(RelationGetSmgr(rel)->smgr_rlocator.locator),
                         HASH_FIND, NULL);

    // Mark as truncated if found
    if (pending)
        pending->is_truncated = true;
}
```