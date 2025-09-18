# RenameRelationInternal

## Location
src/backend/commands/tablecmds.c: 4135 - 4227

## Overview
RenameRelationInternal is the core internal function that performs the actual relation renaming operation, handling all necessary catalog updates and associated object renaming.

## Definition
void RenameRelationInternal(Oid myrelid, const char *newrelname, bool is_internal, bool is_index)

## Detailed Description
RenameRelationInternal implements the low-level mechanics of renaming a database relation by directly updating the pg_class catalog. The function acquires appropriate locks (ShareUpdateExclusiveLock for indexes, AccessExclusiveLock for other relations), checks for name conflicts, and updates the relation name in the system catalog.

Beyond the basic renaming, the function handles associated object renaming including the relation type (for composite types) and constraint names (for indexes with associated constraints). It ensures consistency by maintaining locks until transaction end and invoking post-alter hooks for proper event notification.

## Parameters / Member Variables
- `myrelid`: OID of the relation to rename
- `newrelname`: New name for the relation
- `is_internal`: Whether this is an internal operation (affects hook invocation)
- `is_index`: Whether the relation is an index (affects lock level)

## Dependencies
- Functions called/Symbols referenced:
  - [relation_open](../r/relation_open.md)
  - RelationGetNamespace
  - table_open/table_close
  - [SearchSysCacheLockedCopy1](../S/SearchSysCacheLockedCopy1.md)
  - [get_relname_relid](../g/get_relname_relid.md)
  - namestrcpy
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [UnlockTuple](../U/UnlockTuple.md)
  - InvokeObjectPostAlterHookArg
  - [heap_freetuple](../h/heap_freetuple.md)
  - [RenameTypeInternal](RenameTypeInternal.md)
  - [get_index_constraint](../g/get_index_constraint.md)
  - [RenameConstraintById](RenameConstraintById.md)
  - [relation_close](../r/relation_close.md)
- Called from (representative examples):
  - [RenameRelation](RenameRelation.md) (in src/backend/commands/tablecmds.c)
  - [rename_constraint_internal](../r/rename_constraint_internal.md) (in src/backend/commands/tablecmds.c)
  - [finish_heap_swap](../f/finish_heap_swap.md) (in src/backend/commands/cluster.c)
  - [RenameType](RenameType.md) (in src/backend/commands/typecmds.c)

## Notes and Other Information
- Uses different lock levels based on object type (ShareUpdateExclusiveLock for indexes, AccessExclusiveLock for others)
- Maintains exclusive locks until transaction end to prevent concurrent modifications
- Automatically renames associated composite types when they exist
- Renames associated constraints for indexes that have constraints
- Performs duplicate name checking before proceeding with the rename
- Handles both user-initiated and internal rename operations
- Updates system catalogs directly using low-level catalog functions
- Invokes post-alter hooks for proper event trigger and extension support