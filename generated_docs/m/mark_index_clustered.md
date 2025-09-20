# mark_index_clustered

## Location
[src/backend/commands/cluster.c:560-632](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/cluster.c#L560-L632)

## Overview
Updates the pg_index system catalog to mark a specified index as the clustered index for a table, clearing the clustered flag from all other indexes on the same table.

## Definition

```c
void
mark_index_clustered(Relation rel, Oid indexOid, bool is_internal)
```
## Detailed Description
The mark_index_clustered function manages the indisclustered flag in the pg_index system catalog, which indicates which index (if any) a table is clustered on. This metadata is used by subsequent CLUSTER operations to determine the default clustering index when no specific index is specified.

The function performs several key operations:
1. **Validation**: Ensures the operation is not being applied to a partitioned table (not supported)
2. **Optimization**: Skips the operation if the target index is already marked as clustered
3. **Catalog Update**: Iterates through all indexes on the relation, clearing the indisclustered flag from existing clustered indexes and setting it on the new target index
4. **Hook Invocation**: Triggers post-alter hooks for each modified index to notify extensions and other components

When indexOid is InvalidOid, the function clears the clustered flag from all indexes, effectively removing any clustering designation from the table.

## Parameters / Member Variables
- : Relation structure representing the table whose indexes are being modified
- : OID of the index to mark as clustered, or InvalidOid to clear all clustered flags
- : Boolean flag indicating whether this is an internal operation (affects hook behavior)

## Dependencies
- Functions called/Symbols referenced:
  - [get_index_isclustered](../g/get_index_isclustered.md)
  - table_open/table_close
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - SearchSysCacheCopy1
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHookArg
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [rebuild_relation](../r/rebuild_relation.md)
  - [ATExecClusterOn](../A/ATExecClusterOn.md)
  - [ATExecDropCluster](../A/ATExecDropCluster.md)

## Notes and Other Information
- Explicitly prevents marking indexes as clustered on partitioned tables since clustering is performed at the partition level
- Performs redundant validation on index validity even though this should have been checked earlier, following defensive programming practices
- Uses RowExclusiveLock on pg_index to ensure exclusive access during catalog updates
- Invokes object post-alter hooks for all processed indexes, not just the one being marked clustered, to ensure complete notification coverage
- The function is transactional and will be rolled back if the containing transaction fails