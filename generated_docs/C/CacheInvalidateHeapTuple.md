# CacheInvalidateHeapTuple

## Location
[src/backend/utils/cache/inval.c:1207-1338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L1207-L1338)

## Overview
Registers heap tuples for invalidation at command end, handling both catalog cache and relation cache invalidation for system catalog changes.

## Definition

```c
void
CacheInvalidateHeapTuple(Relation relation,
						 HeapTuple tuple,
						 HeapTuple newtuple)
```
## Detailed Description
This function is called whenever a tuple in a system catalog is inserted, updated, or deleted to ensure proper cache invalidation. It handles two types of invalidation: catalog cache invalidation for tuples that might be cached in catcaches, and relation cache invalidation for tuples that define relation metadata.

The function first validates that invalidation is necessary by checking if the relation is a system catalog (user relations don't affect caches) and excluding TOAST relations. It then prepares invalidation state for the current subtransaction if needed.

For catalog cache invalidation, the function determines whether the relation only invalidates snapshots or requires full cache tuple invalidation. Relations that only affect snapshots (like certain system views) use a simpler invalidation mechanism.

For relation cache invalidation, the function examines specific system catalogs that define relation metadata:
- pg_class: Changes to relation definitions
- pg_attribute: Changes to column definitions  
- pg_index: Changes to index definitions
- pg_constraint: Changes to foreign key constraints

Each catalog requires specific handling to extract the target relation OID and determine the appropriate database scope for the invalidation.

## Parameters / Member Variables
- : The system catalog relation being modified
- : The target tuple for insert/delete operations, or the old tuple version for updates
- : NULL for insert/delete operations, or the new tuple version for updates

## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode
  - [IsCatalogRelation](../I/IsCatalogRelation.md)
  - [IsToastRelation](../I/IsToastRelation.md)
  - [PrepareInvalidationState](../P/PrepareInvalidationState.md)
  - [RelationInvalidatesSnapshotsOnly](../R/RelationInvalidatesSnapshotsOnly.md)
  - [IsSharedRelation](../I/IsSharedRelation.md)
  - [RegisterSnapshotInvalidation](../R/RegisterSnapshotInvalidation.md)
  - [PrepareToInvalidateCacheTuple](../P/PrepareToInvalidateCacheTuple.md)
  - [RegisterCatcacheInvalidation](../R/RegisterCatcacheInvalidation.md)
  - [RegisterRelcacheInvalidation](../R/RegisterRelcacheInvalidation.md)
- Called from (representative examples):
  - [heap_insert](../h/heap_insert.md)
  - [heap_multi_insert](../h/heap_multi_insert.md)
  - [heap_delete](../h/heap_delete.md)
  - [heap_update](../h/heap_update.md)
  - [heap_inplace_update](../h/heap_inplace_update.md)
  - [AlterDomainDropConstraint](../A/AlterDomainDropConstraint.md)
  - [AlterDomainAddConstraint](../A/AlterDomainAddConstraint.md)

## Notes and Other Information
- Does nothing during bootstrap processing mode to avoid circular dependencies
- Only processes system catalog relations, ignoring user tables and TOAST tables
- For updates, called once with both old and new tuple versions to avoid duplicate work
- Uses MyDatabaseId for database-specific invalidations and InvalidOid for shared relations
- Includes a kluge for pg_attribute where shared relations are treated as database-specific due to visibility constraints
- Assumes updates cannot move tuples between different relcache entries
- Foreign key constraints in pg_constraint trigger invalidation of the constrained table
- Essential for maintaining cache consistency across all PostgreSQL backends
- Part of the transactional invalidation system ensuring ACID properties for cached data