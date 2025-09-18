# ATPostAlterTypeCleanup

## Location
src/backend/commands/tablecmds.c: 13840 - 14030

## Overview
ATPostAlterTypeCleanup handles the cleanup phase after ALTER TYPE or SET EXPRESSION operations, dropping and scheduling recreation of all dependent indexes, constraints, and statistics objects.

## Definition
```c
static void ATPostAlterTypeCleanup(List **wqueue, AlteredTableInfo *tab, LOCKMODE lockmode)
```

## Detailed Description
This function performs the critical cleanup phase after column type alterations are complete. It systematically processes all indexes, constraints, and statistics objects that were marked for rebuilding during the type change operation. The function operates in two main phases: first, it re-parses all dependent object definitions and queues their recreation in the work queue; second, it drops all the old objects in a single batch operation. The function handles complex scenarios like foreign key constraints on other tables, inheritance hierarchies, and cross-table dependencies while managing appropriate locking. It also restores special table properties like replica identity and clustering after the objects are recreated.

## Parameters / Member Variables
- `wqueue`: Double pointer to the ALTER TABLE work queue where recreation commands are added
- `tab`: Pointer to AlteredTableInfo structure containing lists of objects to rebuild
- `lockmode`: Lock mode to use for the operations

## Dependencies
- Functions called/Symbols referenced:
  - [new_object_addresses](../n/new_object_addresses.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [get_typ_typrelid](../g/get_typ_typrelid.md)
  - [getBaseType](../g/getBaseType.md)
  - ObjectAddressSet
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - [LockRelationOid](../L/LockRelationOid.md)
  - [ATPostAlterTypeParse](ATPostAlterTypeParse.md)
  - [IndexGetRelation](../I/IndexGetRelation.md)
  - [StatisticsGetRelation](../S/StatisticsGetRelation.md)
  - makeNode
  - [performMultipleDeletions](../p/performMultipleDeletions.md)
  - [free_object_addresses](../f/free_object_addresses.md)
  - [AlteredTableInfo](AlteredTableInfo.md) (struct)
  - ObjectAddresses (struct)
  - Form_pg_constraint (struct)
- Called from (representative examples):
  - [ATRewriteCatalogs](ATRewriteCatalogs.md)
  - child_dependency_type

## Notes and Other Information
- Processes constraints, indexes, and statistics in separate loops with different locking strategies
- Uses AccessExclusiveLock for constraints and indexes, ShareUpdateExclusiveLock for statistics
- Handles inherited constraints by skipping recreation for non-local constraints
- Queues replica identity and cluster property restoration commands for later execution
- Uses DROP_RESTRICT for safety since dependencies should already be handled
- Critical for maintaining database consistency during complex type change operations