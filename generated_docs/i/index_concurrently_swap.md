# index_concurrently_swap

## Location
[src/backend/catalog/index.c:1549-1819](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L1549-L1819)

## Overview
index_concurrently_swap swaps the identity, dependencies, and constraints between a new concurrent index and the old index it's replacing, effectively completing the concurrent index replacement.

## Definition

```c
void
index_concurrently_swap(Oid newIndexId, Oid oldIndexId, const char *oldName)
```
## Detailed Description
This function performs the final phase of concurrent index operations by swapping all metadata between the new and old indexes. It swaps names in pg_class, transfers all constraint flags and validity states in pg_index, moves all associated constraints and triggers to point to the new index, transfers comments, handles partition inheritance relationships, swaps all dependencies, and copies statistics.

The operation is comprehensive, ensuring that the new index takes over the complete identity of the old index while the old index is marked as invalid and ready for cleanup. This includes moving primary key, exclusion, and uniqueness constraints, updating trigger references, and maintaining proper dependency relationships throughout the system.

## Parameters / Member Variables
- : Object identifier of the new index that will replace the old one
- : Object identifier of the old index being replaced
- : Name to assign to the old index after the swap

## Dependencies
- Functions called/Symbols referenced:
  - [relation_open](../r/relation_open.md) (to lock both indexes)
  - SearchSysCacheCopy1 (for catalog tuple retrieval)
  - [namestrcpy](../n/namestrcpy.md) (for name swapping)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (for catalog updates)
  - [heap_freetuple](../h/heap_freetuple.md) (for memory cleanup)
  - [get_index_ref_constraints](../g/get_index_ref_constraints.md)/get_index_constraint (for constraint lookup)
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext (for trigger scanning)
  - [heap_copytuple](../h/heap_copytuple.md)/heap_modify_tuple (for tuple manipulation)
  - [get_rel_relispartition](../g/get_rel_relispartition.md)/get_partition_ancestors (for partition handling)
  - [DeleteInheritsTuple](../D/DeleteInheritsTuple.md)/StoreSingleInheritance (for inheritance updates)
  - [changeDependenciesOf](../c/changeDependenciesOf.md)/changeDependenciesOn (for dependency swapping)
  - [pgstat_copy_relation_stats](../p/pgstat_copy_relation_stats.md) (for statistics transfer)
  - [CopyStatistics](../C/CopyStatistics.md) (for pg_statistic data transfer)
  - [relation_close](../r/relation_close.md) (for cleanup)
- Called from (representative examples):
  - Concurrent reindex completion operations

## Notes and Other Information
- This is a void function that performs extensive catalog modifications
- Uses ShareUpdateExclusiveLock on both indexes to prevent concurrent modifications
- Swaps names, constraint flags, validity states, and partition flags between indexes
- Moves all constraints (primary key, unique, exclusion) to the new index
- Updates trigger constraint references to point to the new index
- Transfers comments from old to new index via pg_description updates
- Handles partition inheritance by updating pg_inherits relationships
- Performs complete dependency swapping to maintain referential integrity
- Copies relation statistics and pg_statistic data to the new index
- Marks the old index as invalid while making the new index valid and ready
- Does not call CommandCounterIncrement() to avoid duplicate pg_depend entries
- Maintains locks until transaction end but closes relations immediately
- Located at src/backend/catalog/index.c:1549-1819