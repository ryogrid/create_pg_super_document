# update_relispartition

## Location
[src/backend/commands/indexcmds.c:4436-4473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L4436-L4473)

## Overview
update_relispartition updates the relispartition flag in the pg_class system catalog to indicate whether a relation is a partition or not.

## Definition
```c
static void update_relispartition(Oid relationId, bool newval)
```

## Detailed Description
This static helper function modifies the relispartition field of a relation's pg_class catalog entry to reflect its current partition status. The function performs an in-place update of the pg_class tuple, ensuring proper locking and cache invalidation.

The function follows PostgreSQL's standard pattern for updating system catalogs:
- Opens the pg_class relation with exclusive access
- Retrieves and locks the target tuple from the system cache
- Validates that the tuple exists and the new value differs from the current value
- Updates the relispartition field directly in the tuple structure
- Commits the change to the catalog and releases locks
- Properly manages memory by freeing the tuple when done

This function is specifically designed as a subroutine for IndexSetParentIndex and handles the catalog-level mechanics of updating partition status.

## Parameters / Member Variables
- `relationId`: OID of the relation whose relispartition flag should be updated
- `newval`: Boolean value to set for the relispartition field (true for partitions, false for non-partitions)

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [SearchSysCacheLockedCopy1](../S/SearchSysCacheLockedCopy1.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [UnlockTuple](../U/UnlockTuple.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - table_close
  - Form_pg_class (structure)
- Called from (representative examples):
  - [IndexSetParentIndex](../I/IndexSetParentIndex.md)

## Notes and Other Information
- The function is static (internal to indexcmds.c) and serves as a specialized helper for partition management
- Uses SearchSysCacheLockedCopy1 to obtain a locked copy of the tuple, ensuring safe concurrent access
- Includes an assertion to verify that the new value actually differs from the current value, helping catch programming errors
- Uses RowExclusiveLock on the pg_class relation to prevent concurrent modifications
- Properly manages tuple locks with InplaceUpdateTupleLock to ensure consistency during the update
- The function will throw an ERROR if the specified relation doesn't exist in the catalog
- Memory management is handled correctly by freeing the heap tuple after the update is complete