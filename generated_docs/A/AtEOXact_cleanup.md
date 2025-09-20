# AtEOXact_cleanup

## Location
[src/backend/utils/cache/relcache.c:3307-3388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L3307-L3388)

## Overview
Performs cleanup operations on a single relation at main-transaction commit or abort, managing reference counts, subtransaction IDs, and determining whether to clear the relcache entry.

## Definition

```c
static void
AtEOXact_cleanup(Relation relation, bool isCommit)
```
## Detailed Description
This static function handles the cleanup of individual relation cache entries during transaction termination. It performs several critical operations:

1. **Reference Count Validation**: In assert-enabled builds, verifies that the relation's reference count has returned to its expected state (0 for regular relations, 1 for nailed relations) when not in bootstrap mode.

2. **Relcache Entry Lifecycle Management**: Determines whether the relcache entry should be cleared based on the transaction outcome:
   - During commit: Clears entries for relations that were dropped (rd_droppedSubid != InvalidSubTransactionId)
   - During rollback: Clears entries for relations that were created in the current transaction (rd_createSubid != InvalidSubTransactionId)

3. **Subtransaction ID Reset**: Resets all subtransaction-related fields to InvalidSubTransactionId to indicate the relation is no longer associated with the current transaction.

4. **Safe Entry Removal**: Attempts to clear the relcache entry if appropriate, but only if the reference count is zero to avoid dangling pointer issues.

The function is designed to be idempotent since EOXactListAdd() doesn't prevent duplicate entries in the eoxact_list[].

## Parameters / Member Variables
- : The Relation object to be cleaned up
- : Boolean indicating whether this cleanup is happening at commit (true) or abort (false)

## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode
  - RelationHasReferenceCountZero
  - [RelationClearRelation](../R/RelationClearRelation.md)
  - RelationGetRelationName
  - elog
- Constants used:
  - InvalidSubTransactionId
- [Relation](../R/Relation.md) fields accessed:
  - rd_refcnt
  - rd_isnailed
  - rd_createSubid
  - rd_newRelfilelocatorSubid
  - rd_firstRelfilelocatorSubid
  - rd_droppedSubid
- Called from:
  - [AtEOXact_RelationCache](AtEOXact_RelationCache.md) (twice in different code paths)

## Notes and Other Information
- This is a static (internal) function within relcache.c
- The function must be idempotent to handle potential duplicate entries in the eoxact_list
- Bootstrap mode bypasses reference count checking since bootstrap code expects relations to stay open across transaction boundaries
- If a relation has a non-zero reference count when it should be cleared, the function logs a WARNING instead of failing to prevent error-during-error-recovery loops
- The function resets all subtransaction IDs regardless of the commit/abort status to ensure clean state
- Reference count validation is only performed in assert-enabled builds for performance reasons