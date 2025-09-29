# AssertPendingSyncConsistency

## Location
[src/backend/utils/cache/relcache.c:3144-3165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L3144-L3165)

## Overview
Validates the consistency between relation cache state and WAL-skipping behavior for relations with pending synchronization requirements.

## Definition
```c
static void AssertPendingSyncConsistency(Relation relation)
```

## Detailed Description
AssertPendingSyncConsistency is a debugging and validation function that ensures the relation cache correctly tracks relations that should be skipping WAL (Write-Ahead Logging) due to being newly created or having new relfilelocators within the current transaction.

The function performs several critical consistency checks:

1. **WAL-skipping logic verification**: It calculates whether a relation should be skipping WAL based on relation cache state (permanent relation with either rd_createSubid or rd_firstRelfilelocatorSubid set for storage-having relations) and asserts this matches the result from RelFileLocatorSkippingWAL().

2. **Dropped relation state validation**: For relations marked as dropped (rd_droppedSubid set), it verifies that:
   - The relation is marked as invalid (rd_isvalid is false)
   - The relation was either created in this transaction or had its relfilelocator changed in this transaction

This function helps ensure the correctness of PostgreSQL's optimization where certain relations can skip WAL logging during their creation transaction, which is safe because they can be recreated if the transaction aborts.

## Parameters / Member Variables
- `relation`: The Relation structure to validate for pending sync consistency

## Dependencies
- Functions called/Symbols referenced:
  - RelationIsPermanent
  - RELKIND_HAS_STORAGE
  - [RelFileLocatorSkippingWAL](../R/RelFileLocatorSkippingWAL.md)
- Called from (representative examples):
  - [AssertPendingSyncs_RelationCache](AssertPendingSyncs_RelationCache.md)

## Notes and Other Information
- This is a static function, only accessible within the relcache.c module
- Used primarily for debugging and validation in assertion builds
- The function is part of PostgreSQL's pending sync mechanism, which optimizes WAL usage for newly created relations
- The consistency checks help ensure that relation cache state accurately reflects the WAL-skipping status
- Related to the "RelFileLocatorSkippingWAL" functionality mentioned in the provided context
- Critical for maintaining data consistency in PostgreSQL's transaction and recovery system

## Simplified Source

```c
static void AssertPendingSyncConsistency(Relation relation)
{
    bool relcache_verdict =
        RelationIsPermanent(relation) &&
        ((relation->rd_createSubid != InvalidSubTransactionId &&
          RELKIND_HAS_STORAGE(relation->rd_rel->relkind)) ||
         relation->rd_firstRelfilelocatorSubid != InvalidSubTransactionId);

    Assert(relcache_verdict == RelFileLocatorSkippingWAL(relation->rd_locator));

    if (relation->rd_droppedSubid != InvalidSubTransactionId)
        Assert(!relation->rd_isvalid &&
               (relation->rd_createSubid != InvalidSubTransactionId ||
                relation->rd_firstRelfilelocatorSubid != InvalidSubTransactionId));
}
```