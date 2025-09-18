# ATExecAlterConstrRecurse

## Location
src/backend/commands/tablecmds.c: 11553 - 11703

## Overview
ATExecAlterConstrRecurse is a recursive subroutine of ATExecAlterConstraint that performs the actual constraint modification work, including updating constraint and trigger catalog entries and recursively processing child constraints in partitioned tables.

## Definition


## Detailed Description
This function handles the core logic of constraint alteration by updating both constraint and trigger catalog entries. It modifies the deferrability and initial deferred status of foreign key constraints in the pg_constraint catalog, then updates the corresponding triggers in pg_trigger that implement the constraint. The function also handles partitioned tables by recursively processing all child constraints to ensure consistency across the partition hierarchy.

Key operations include:
1. Updates the constraint tuple in pg_constraint if attributes have changed
2. Scans and updates all related triggers that implement the constraint
3. Tracks other relations involved for cache invalidation
4. Recursively processes child constraints in partitioned table hierarchies
5. Returns whether any changes were actually made

## Parameters / Member Variables
- : The constraint specification containing new attribute values
- : Open relation handle for the pg_constraint catalog
- : Open relation handle for the pg_trigger catalog  
- : The relation containing the constraint being altered
- : The constraint tuple from pg_constraint being modified
- : List to collect OIDs of other relations with affected triggers
- : Lock mode to use when opening child relations

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth
  - [heap_copytuple](../h/heap_copytuple.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [list_append_unique_oid](../l/list_append_unique_oid.md)
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - table_open
  - table_close
  - [ATExecAlterConstrRecurse](ATExecAlterConstrRecurse.md) (recursive self-call)
- Called from (representative examples):
  - [ATExecAlterConstraint](ATExecAlterConstraint.md) (main constraint alteration function)
  - [ATExecAlterConstrRecurse](ATExecAlterConstrRecurse.md) (recursive calls for child constraints)

## Notes and Other Information
- Only updates specific trigger types (RI_FKey_noaction_del, RI_FKey_noaction_upd, RI_FKey_check_ins, RI_FKey_check_upd)
- Uses stack depth checking to prevent overflow during deep recursion
- Must recurse even when constraint values are already correct to handle partitions that may have been altered locally
- Collects OIDs of other relations for cache invalidation to maintain consistency
- Returns true if any actual changes were made to the constraint or its triggers