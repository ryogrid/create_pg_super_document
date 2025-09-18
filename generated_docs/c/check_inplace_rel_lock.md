# check_inplace_rel_lock

## Location
src/backend/access/heap/heapam.c: 4265 - 4302

## Overview
check_inplace_rel_lock is a static validation function that confirms adequate relation-level locking is held when performing inplace operations on system catalog relations, specifically for pg_class entries.

## Definition


## Detailed Description
This function validates that proper relation-level locks are held when performing inplace operations on catalog tuples, implementing the locking rules documented in README.tuplock section "Locking to write inplace-updated tables". Unlike check_lock_if_inplace_updateable_rel which handles multiple catalog types, this function focuses specifically on validating relation-level locks for pg_class entries.

The function extracts relation information from the provided pg_class tuple and constructs the appropriate lock tag for validation. It handles several important cases:

**Shared vs Database Relations**: Determines whether the relation is shared across databases (using InvalidOid as database ID) or specific to the current database (using MyDatabaseId).

**Index Relations**: For index entries in pg_class, validates locks on the underlying table rather than the index itself, since index operations typically require locks on the base table.

**Regular Relations**: For non-index relations, validates locks directly on the relation identified by the pg_class tuple.

The function specifically checks for ShareUpdateExclusiveLock, which provides sufficient protection for inplace catalog operations while allowing concurrent readers. When the required lock is not held, it generates a WARNING message with detailed diagnostic information including relation name, OID, relation kind, and tuple location.

This is a debug-time validation mechanism that helps developers ensure proper locking protocols are followed during catalog manipulation operations.

## Parameters / Member Variables
- : HeapTuple representing a pg_class catalog entry for which relation-level lock validation is needed

## Dependencies
- Functions called/Symbols referenced:
  - [IsSharedRelation](../I/IsSharedRelation.md)
  - [index_open](../i/index_open.md)
  - [index_close](../i/index_close.md)
  - SET_LOCKTAG_RELATION
  - [LockHeldByMe](../L/LockHeldByMe.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
- Called from (representative examples):
  - [heap_inplace_lock](../h/heap_inplace_lock.md)

## Notes and Other Information
- Designed specifically for pg_class catalog entries, unlike the more general check_lock_if_inplace_updateable_rel
- Validates ShareUpdateExclusiveLock specifically, which is the standard lock level for catalog modifications
- For index relations, validates locks on the underlying table rather than the index catalog entry itself
- Generates WARNING messages rather than errors, making this a diagnostic tool for development
- The function assumes the tuple is from pg_class and contains valid relation metadata
- Helps ensure the complex locking protocols required for safe catalog inplace updates are properly followed
- Used in conjunction with inplace update operations that bypass normal MVCC versioning for performance reasons
- Essential for preventing corruption in system catalogs during concurrent operations