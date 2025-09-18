# RelationDecrementReferenceCount

## Location
src/backend/utils/cache/relcache.c: 2174 - 2193

## Overview
Decrements the reference count for a given relation and removes the reference tracking from the current resource owner.

## Definition
```c
void RelationDecrementReferenceCount(Relation rel)
```

## Detailed Description
This function safely decrements the reference count (rd_refcnt) of a relation structure by one and removes the reference tracking from the current resource owner. It serves as the counterpart to RelationIncrementReferenceCount and is essential for proper relation lifecycle management.

The function includes an assertion to ensure the reference count is positive before decrementing, preventing underflow errors. Like its increment counterpart, it handles bootstrap mode specially by skipping resource owner tracking during database initialization.

## Parameters / Member Variables
- `rel`: Pointer to the Relation structure whose reference count should be decremented

## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode
  - ResourceOwnerForgetRelationRef
- Called from (representative examples):
  - heap_endscan
  - index_endscan
  - DestroyPartitionDirectory
  - RelationClose

## Notes and Other Information
- Contains an assertion (Assert(rel->rd_refcnt > 0)) to catch reference count underflow bugs during development
- Bootstrap mode handling matches RelationIncrementReferenceCount by skipping resource owner operations
- This function is critical for preventing memory leaks by ensuring relations are properly cleaned up when no longer referenced
- Must be called exactly once for each corresponding RelationIncrementReferenceCount call
- When the reference count reaches zero, the relation becomes eligible for cleanup by the cache management system