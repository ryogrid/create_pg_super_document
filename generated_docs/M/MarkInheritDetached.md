# MarkInheritDetached

## Location
[src/backend/commands/tablecmds.c:16183-16265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L16183-L16265)

## Overview
Marks a partition as pending detach in concurrent mode for ATExecDetachPartition, while ensuring no other partitions are already pending detach.

## Definition

```c
static void
MarkInheritDetached(Relation child_rel, Relation parent_rel)
```
## Detailed Description
MarkInheritDetached is a utility function used by the concurrent partition detachment process. It scans all inheritance entries for a given parent table to find the specified child partition and sets its inhdetachpending flag to true in the pg_inherits catalog. During this process, it also validates that no other partition of the same parent table is already marked as pending detach, as PostgreSQL allows only one concurrent detach operation per partitioned table at a time. The function operates under a RowExclusiveLock on the pg_inherits catalog to ensure consistency during the concurrent operation.

## Parameters / Member Variables
- : The partition relation that is being marked for detachment
- : The partitioned table relation from which the child is being detached

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
  - [get_rel_name](../g/get_rel_name.md)
  - [get_namespace_name](../g/get_namespace_name.md)
- Called from (representative examples):
  - [ATExecDetachPartition](../A/ATExecDetachPartition.md)

## Notes and Other Information
- Requires that the parent relation is a partitioned table (asserted with relkind check)
- Scans all partitions of the parent table to ensure only one detach operation is pending at a time
- Uses InheritsParentIndexId for efficient scanning of pg_inherits entries
- Sets the inhdetachpending flag in the pg_inherits catalog entry for the specified child partition
- Provides helpful error messages with suggestions to use FINALIZE if another partition is already pending detach
- Validates that the child relation is actually a partition of the specified parent before proceeding