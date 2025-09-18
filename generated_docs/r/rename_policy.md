# rename_policy

## Location
src/backend/commands/policy.c: 1096 - 1203

## Overview
Changes the name of a policy on a relation by updating the policy's name in the pg_policy system catalog while ensuring the new name doesn't conflict with existing policies on the same table.

## Definition


## Detailed Description
This function implements policy renaming through a two-phase process to ensure data consistency:

1. **Conflict Detection Phase**: First scans pg_policy to check if a policy with the target name already exists on the same table, raising an error if found
2. **Rename Phase**: Locates the policy with the old name and updates its name field in the catalog
3. **Atomic Operation**: Uses proper catalog locking (RowExclusiveLock) to prevent concurrent modifications during the rename process
4. **Cache Invalidation**: Invalidates the relation's cache entry to ensure all backends rebuild their policy information
5. **Event Notification**: Triggers post-alter hooks for proper event handling

The function operates directly on the catalog tuple by copying it, modifying the name field, and updating it back to the catalog.

## Parameters
- : RenameStmt structure containing:
  - : Target table (RangeVar) containing the policy to rename
  - : Current name of the policy to be renamed
  - : New name for the policy
  - Other RenameStmt fields (not directly used for policies)

## Dependencies
- Functions called/Symbols referenced:
  - RangeVarGetRelidExtended, RangeVarCallbackForPolicy (table identification and permissions)
  - relation_open, table_open, relation_close, table_close (relation management)
  - systable_beginscan, systable_getnext, systable_endscan (catalog scanning)
  - heap_copytuple (tuple duplication)
  - namestrcpy (name field modification)
  - CatalogTupleUpdate (catalog updates)
  - InvokeObjectPostAlterHook (event hooks)
  - CacheInvalidateRelcache (cache management)
  - ObjectAddressSet (return value construction)
- Called from:
  - ExecRenameStmt (main rename command dispatcher)

## Notes and Other Information
- Requires AccessExclusiveLock on the target table to prevent concurrent DDL operations
- Policy names must be unique within each table but can be duplicated across different tables
- Uses two separate catalog scans for safety: first to check conflicts, second to perform the rename
- Does not affect policy dependencies or other attributes - only the name is changed
- Returns ObjectAddress of the renamed policy for use by the event system
- The function ensures proper cleanup of scan resources and relation locks in all code paths
- Cache invalidation ensures that query plans using the renamed policy are recompiled