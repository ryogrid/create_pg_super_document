# RelationIncrementReferenceCount

## Location
[src/backend/utils/cache/relcache.c:2161-2173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L2161-L2173)

## Overview
Increments the reference count for a given relation and tracks the reference in the current resource owner for proper cleanup.

## Definition

```c
void
RelationIncrementReferenceCount(Relation rel)
```
## Detailed Description
This function safely increments the reference count () of a relation structure by one and ensures that the current resource owner tracks this reference for proper cleanup. The function handles the special case of bootstrap mode, where reference count ownership tracking is disabled due to different initialization patterns during database bootstrap.

The function follows a two-step process:
1. Ensures the current resource owner has enough capacity to track an additional relation reference
2. Increments the relation's reference count and registers it with the resource owner (except in bootstrap mode)

## Parameters / Member Variables
- `rel`: Pointer to the Relation structure whose reference count should be incremented
## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerEnlarge](ResourceOwnerEnlarge.md)
  - IsBootstrapProcessingMode
  - [ResourceOwnerRememberRelationRef](ResourceOwnerRememberRelationRef.md)
- Called from (representative examples):
  - [heap_beginscan](../h/heap_beginscan.md)
  - [index_beginscan_internal](../i/index_beginscan_internal.md)
  - [PartitionDirectoryLookup](../P/PartitionDirectoryLookup.md)
  - [RelationIdGetRelation](RelationIdGetRelation.md)

## Notes and Other Information
- Bootstrap mode has special handling where reference count ownership tracking is disabled due to different relation lifecycle management during database initialization
- The function ensures resource owner capacity before making changes, preventing memory allocation failures during reference tracking
- This is part of PostgreSQL's reference counting mechanism to prevent premature cleanup of actively used relations
- Always paired with RelationDecrementReferenceCount when the relation reference is no longer needed

## Simplified Source

```c
void
RelationIncrementReferenceCount(Relation rel) {
    // Ensure resource owner can track one more relation reference
    ResourceOwnerEnlarge(CurrentResourceOwner);

    // Increment the relation's reference count
    rel->rd_refcnt += 1;

    // Track this reference for cleanup (except during bootstrap)
    if (!IsBootstrapProcessingMode())
        ResourceOwnerRememberRelationRef(CurrentResourceOwner, rel);
}
```