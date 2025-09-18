# ATExecDetachPartition

## Location
src/backend/commands/tablecmds.c: 19141 - 19319

## Overview
ATExecDetachPartition implements the ALTER TABLE DETACH PARTITION command, removing the inheritance relationship between a partition and its parent table, with support for both immediate and concurrent detaching modes.

## Definition
```c
static ObjectAddress ATExecDetachPartition(List **wqueue, AlteredTableInfo *tab, Relation rel, RangeVar *name, bool concurrent)
```

## Detailed Description
This function handles the detachment of a partition from its parent partitioned table. It supports two modes of operation:

1. **Non-concurrent mode**: Immediately removes the partition relationship, requiring exclusive locks that may block concurrent operations.

2. **Concurrent mode**: Uses a two-transaction approach to minimize blocking:
   - First transaction: Marks the partition as pending detach and adds necessary constraints
   - Second transaction: Waits for all existing queries to complete, then finalizes the detachment

The function ensures referential integrity is maintained during detachment and handles special cases like default partitions. In concurrent mode, it cannot operate when a default partition exists due to constraint management complexities.

## Parameters / Member Variables
- `wqueue`: Pointer to the ALTER TABLE work queue list for managing related operations
- `tab`: AlteredTableInfo structure containing information about the table being altered
- `rel`: The parent partitioned table relation
- `name`: RangeVar specifying the partition to be detached
- `concurrent`: Boolean flag indicating whether to use concurrent detaching mode

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetPartitionDesc
  - get_default_oid_from_partdesc
  - LockRelationOid
  - table_openrv
  - RemoveInheritance
  - MarkInheritDetached
  - ATDetachCheckNoForeignKeyRefs
  - DetachAddConstraintIfNeeded
  - DetachPartitionFinalize
  - CommitTransactionCommand/StartTransactionCommand
  - WaitForLockersMultiple
- Called from (representative examples):
  - ATExecCmd (main ALTER TABLE command dispatcher)

## Notes and Other Information
- Cannot run concurrently when a default partition exists due to constraint management issues
- Uses different locking strategies: ShareUpdateExclusiveLock in concurrent mode, AccessExclusiveLock otherwise
- The concurrent approach requires careful transaction management to ensure consistency
- Maintains foreign key integrity throughout the detachment process
- Returns ObjectAddress of the detached relation for further processing