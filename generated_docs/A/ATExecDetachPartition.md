# ATExecDetachPartition

## Location
[src/backend/commands/tablecmds.c:19141-19319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L19141-L19319)

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
  - [RelationGetPartitionDesc](../R/RelationGetPartitionDesc.md)
  - [get_default_oid_from_partdesc](../g/get_default_oid_from_partdesc.md)
  - [LockRelationOid](../L/LockRelationOid.md)
  - [table_openrv](../t/table_openrv.md)
  - [RemoveInheritance](../R/RemoveInheritance.md)
  - [MarkInheritDetached](../M/MarkInheritDetached.md)
  - [ATDetachCheckNoForeignKeyRefs](ATDetachCheckNoForeignKeyRefs.md)
  - [DetachAddConstraintIfNeeded](../D/DetachAddConstraintIfNeeded.md)
  - [DetachPartitionFinalize](../D/DetachPartitionFinalize.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)/StartTransactionCommand
  - [WaitForLockersMultiple](../W/WaitForLockersMultiple.md)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command dispatcher)

## Notes and Other Information
- Cannot run concurrently when a default partition exists due to constraint management issues
- Uses different locking strategies: ShareUpdateExclusiveLock in concurrent mode, AccessExclusiveLock otherwise
- The concurrent approach requires careful transaction management to ensure consistency
- Maintains foreign key integrity throughout the detachment process
- Returns ObjectAddress of the detached relation for further processing

## Simplified Source

```c
static ObjectAddress
ATExecDetachPartition(List **wqueue, AlteredTableInfo *tab, Relation rel,
                      RangeVar *name, bool concurrent)
{
    Relation partRel;
    ObjectAddress address;
    Oid defaultPartOid;

    // Check if default partition exists - concurrent mode not supported with defaults
    defaultPartOid = get_default_oid_from_partdesc(RelationGetPartitionDesc(rel, true));
    if (OidIsValid(defaultPartOid))
    {
        if (concurrent)
            ereport(ERROR, (errmsg("cannot detach partitions concurrently when a default partition exists")));
        LockRelationOid(defaultPartOid, AccessExclusiveLock);
    }

    // Open partition with appropriate lock mode
    partRel = table_openrv(name, concurrent ? ShareUpdateExclusiveLock : AccessExclusiveLock);

    // Remove inheritance relationship
    if (!concurrent)
        RemoveInheritance(partRel, rel, false);
    else
        MarkInheritDetached(partRel, rel);  // Mark as pending detach

    // Ensure foreign key constraints still hold
    ATDetachCheckNoForeignKeyRefs(partRel);

    // Handle concurrent mode with two-transaction approach
    if (concurrent)
    {
        Oid partrelid = RelationGetRelid(partRel);
        Oid parentrelid = RelationGetRelid(rel);

        // Add constraint to partition if needed
        DetachAddConstraintIfNeeded(wqueue, partRel);

        // Close relations and commit first transaction
        table_close(partRel, NoLock);
        table_close(rel, NoLock);
        tab->rel = NULL;

        PopActiveSnapshot();
        CommitTransactionCommand();
        StartTransactionCommand();

        // Wait for all queries seeing the old state to complete
        LOCKTAG tag;
        SET_LOCKTAG_RELATION(tag, MyDatabaseId, parentrelid);
        WaitForLockersMultiple(list_make1(&tag), AccessExclusiveLock, false);

        // Reopen relations
        rel = try_relation_open(parentrelid, ShareUpdateExclusiveLock);
        partRel = try_relation_open(partrelid, AccessExclusiveLock);

        if (rel == NULL || partRel == NULL)
            ereport(ERROR, (errmsg("relation was removed concurrently")));

        tab->rel = rel;
    }

    // Complete the detachment process
    PushActiveSnapshot(GetTransactionSnapshot());
    DetachPartitionFinalize(rel, partRel, concurrent, defaultPartOid);
    PopActiveSnapshot();

    ObjectAddressSet(address, RelationRelationId, RelationGetRelid(partRel));
    table_close(partRel, NoLock);
    return address;
}
```