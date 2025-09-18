# RelationForgetRelation

## Location
src/backend/utils/cache/relcache.c: 2913 - 2956

## Overview
Handles the removal of a relation from the relation cache when the caller reports that the relation has been dropped, ensuring proper cleanup while preserving subtransaction state information.

## Definition
```c
void RelationForgetRelation(Oid rid)
```

## Detailed Description
RelationForgetRelation is called when a relation has been dropped and needs to be removed from the relation cache. The function performs several critical safety checks before proceeding with the cleanup:

1. First checks if the relation exists in the cache using RelationIdCacheLookup
2. Verifies that the relation has no active references (reference count is zero)
3. Handles subtransaction scenarios by marking the relation as "dropped" rather than immediately destroying it when the relation was created or had its relfilelocator changed within a subtransaction
4. Finally calls RelationClearRelation to invalidate the cache entry

The function is designed to be safe for both top-level transactions and subtransactions, preserving necessary state information for potential rollback scenarios.

## Parameters / Member Variables
- `rid`: The OID of the relation to forget/remove from the cache

## Dependencies
- Functions called/Symbols referenced:
  - RelationIdCacheLookup
  - PointerIsValid
  - RelationHasReferenceCountZero
  - [GetCurrentSubTransactionId](../G/GetCurrentSubTransactionId.md)
  - [RelationClearRelation](RelationClearRelation.md)
- Called from (representative examples):
  - [heap_drop_with_catalog](../h/heap_drop_with_catalog.md)
  - [index_drop](../i/index_drop.md)

## Notes and Other Information
- The function includes an assertion that rd_droppedSubid must be InvalidSubTransactionId when called
- If the relation still has active references, it raises an ERROR rather than proceeding
- Special handling for subtransaction scenarios: relations created or modified within subtransactions are marked as "dropped" rather than immediately destroyed to support rollback operations
- The function is part of PostgreSQL's relation cache management system, ensuring cache consistency when relations are dropped