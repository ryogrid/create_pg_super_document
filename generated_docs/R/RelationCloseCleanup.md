# RelationCloseCleanup

## Location
[src/backend/utils/cache/relcache.c:2203-2256](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L2203-L2256)

## Overview
Performs cleanup operations for relations whose reference count has reached zero, including partition descriptor cleanup and conditional cache clearing.

## Definition
```c
static void RelationCloseCleanup(Relation relation)
```

## Detailed Description
RelationCloseCleanup is an internal function responsible for cleaning up resources associated with relations that are no longer actively referenced. It performs two main types of cleanup:

1. **Partition Descriptor Cleanup**: When a relation's reference count reaches zero, it cleans up any stale partition descriptor child contexts to free memory. This optimization only occurs when child contexts exist, avoiding unnecessary function calls.

2. **Debug Mode Cache Clearing**: When compiled with RELCACHE_FORCE_RELEASE, it aggressively clears relation cache entries that have zero references and were not created in the current transaction or subtransaction.

The function is designed to be efficient, checking for the existence of child memory contexts before attempting to delete them, and only performing expensive operations when necessary.

## Parameters / Member Variables
- `relation`: Pointer to the Relation structure to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - RelationHasReferenceCountZero
  - [MemoryContextDeleteChildren](../M/MemoryContextDeleteChildren.md)
  - InvalidSubTransactionId
  - [RelationClearRelation](RelationClearRelation.md)
- Called from (representative examples):
  - [RelationClose](RelationClose.md)
  - [ResOwnerReleaseRelation](ResOwnerReleaseRelation.md)

## Notes and Other Information
- This is a static function, only callable from within relcache.c
- Partition descriptor cleanup (rd_pdcxt and rd_pddcxt) helps manage memory for partitioned tables
- The RELCACHE_FORCE_RELEASE behavior is only active in debug builds and helps catch use-after-free bugs
- Only cleans up relations that weren't created in the current transaction to avoid premature cleanup
- The cleanup is opportunistic - it only occurs when reference counts naturally drop to zero
- Child context existence is checked before deletion to avoid unnecessary mcxt.c calls

## Simplified Source

```c
static void RelationCloseCleanup(Relation relation) {
    // Clean up partition descriptor contexts if relation has zero references
    if (RelationHasReferenceCountZero(relation)) {
        // Clean up partition descriptor child contexts to free memory
        if (relation->rd_pdcxt != NULL &&
            relation->rd_pdcxt->firstchild != NULL)
            MemoryContextDeleteChildren(relation->rd_pdcxt);

        if (relation->rd_pddcxt != NULL &&
            relation->rd_pddcxt->firstchild != NULL)
            MemoryContextDeleteChildren(relation->rd_pddcxt);
    }

#ifdef RELCACHE_FORCE_RELEASE
    // In debug builds, aggressively clear cache entries with zero references
    // that weren't created in current transaction/subtransaction
    if (RelationHasReferenceCountZero(relation) &&
        relation->rd_createSubid == InvalidSubTransactionId &&
        relation->rd_firstRelfilelocatorSubid == InvalidSubTransactionId)
        RelationClearRelation(relation, false);
#endif
}
```