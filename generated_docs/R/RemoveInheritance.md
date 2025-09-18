# RemoveInheritance

## Location
src/backend/commands/tablecmds.c: 16266 - 16433

## Overview
Removes inheritance relationship between a child and parent table by adjusting column and constraint inheritance counters and removing catalog entries.

## Definition


## Detailed Description
RemoveInheritance implements the core logic for breaking inheritance relationships between tables. It performs several critical operations: deletes the pg_inherits tuple, decrements attinhcount for inherited attributes and sets attislocal to true when the count reaches zero, similarly handles inherited check constraints by decrementing coninhcount and setting conislocal appropriately, and removes dependency entries between the child and parent relations. The function maintains PostgreSQL's inheritance semantics where once a column becomes local (attislocal=true), it remains local even if inheritance is re-established later, preventing unexpected data loss from automatic column drops.

## Parameters / Member Variables
- : The child relation from which inheritance is being removed
- : The parent relation being removed from the inheritance hierarchy
- : Flag passed to DeleteInheritsTuple indicating whether the inheritance tuple is expected to be marked as detached

## Dependencies
- Functions called/Symbols referenced:
  - DeleteInheritsTuple
  - table_open
  - systable_beginscan
  - systable_getnext
  - SearchSysCacheExistsAttName
  - heap_copytuple
  - CatalogTupleUpdate
  - heap_freetuple
  - drop_parent_dependency
  - child_dependency_type
  - InvokeObjectPostAlterHookArg
- Called from (representative examples):
  - ATExecDropInherit
  - ATExecDetachPartition
  - DetachPartitionFinalize

## Notes and Other Information
- Used by both ATExecDropInherit (regular inheritance) and ATExecDetachPartition (partition detachment)
- Handles different error messages for partitioned tables vs regular inheritance relationships
- Processes both attributes and check constraints, ensuring their inheritance counters are properly decremented
- Uses name matching for constraint inheritance removal, assuming expression matching follows
- Sets attislocal/conislocal to true when inheritance count reaches zero, ensuring columns/constraints become permanently local
- Invokes post-alter hooks with the parent relation OID as auxiliary information
- Maintains catalog consistency by operating under RowExclusiveLock on affected system catalogs