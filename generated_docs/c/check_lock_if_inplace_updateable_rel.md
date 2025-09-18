# check_lock_if_inplace_updateable_rel

## Location
src/backend/access/heap/heapam.c: 4182 - 4264

## Overview
check_lock_if_inplace_updateable_rel is a static validation function that confirms adequate locking is held during heap_update operations on inplace-updateable system catalog relations to prevent corruption.

## Definition


## Detailed Description
This function implements critical safety checks for heap updates on specific system catalog tables that support inplace updates - a special optimization where certain catalog updates can modify tuples without creating new tuple versions. This optimization requires careful coordination to prevent corruption from concurrent access.

The function validates locking according to the rules documented in README.tuplock section "Locking to write inplace-updated tables". It performs different checks based on the specific catalog being updated:

**For pg_class (RelationRelationId)**: Checks for either tuple-level locks (LOCKTAG_TUPLE) or appropriate relation-level locks (ShareUpdateExclusiveLock or ShareRowExclusiveLock). For index relations, it validates locks on the underlying table rather than the index itself.

**For pg_database (DatabaseRelationId)**: Requires tuple-level locks (LOCKTAG_TUPLE) as relation-level locking is not sufficient for database catalog updates.

The function is compiled only in assertion-enabled builds (USE_ASSERT_CHECKING), making it a debug-time validation tool rather than a runtime safety mechanism.

When inadequate locking is detected, the function logs WARNING messages with detailed information about the missing lock, including relation name, OID, relation kind, and tuple identifier for debugging purposes.

## Parameters / Member Variables
- : The heap relation being updated (must be an inplace-updateable catalog)
- : ItemPointer identifying the location of the tuple being updated
- : HeapTuple containing the new tuple data (used to extract relation information for validation)

## Dependencies
- Functions called/Symbols referenced:
  - [LockHeldByMe](../L/LockHeldByMe.md)
  - [IsInplaceUpdateRelation](../I/IsInplaceUpdateRelation.md)
  - [IsSharedRelation](../I/IsSharedRelation.md)
  - [index_open](../i/index_open.md)
  - [index_close](../i/index_close.md)
  - SET_LOCKTAG_TUPLE
  - SET_LOCKTAG_RELATION
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
- Called from (representative examples):
  - [heap_update](../h/heap_update.md)

## Notes and Other Information
- Only compiled in assertion-enabled builds (USE_ASSERT_CHECKING), not in production
- Implements safety checks for the inplace update optimization used on system catalogs
- Different catalog tables have different locking requirements based on their update patterns
- For pg_class updates, validates locks on the actual relation being modified, not the catalog entry
- Index relations require locks on their underlying table rather than the index itself
- Generates WARNING messages rather than errors, making this a diagnostic tool
- The function assumes the relation is already known to be inplace-updateable
- Helps developers detect insufficient locking that could lead to catalog corruption during concurrent operations