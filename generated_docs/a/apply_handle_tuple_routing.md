# apply_handle_tuple_routing

## Location
[src/backend/replication/logical/worker.c:2908-3156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L2908-L3156)

## Overview
Handles insert, update, and delete operations on partitioned tables in PostgreSQL's logical replication worker, managing tuple routing to appropriate partitions.

## Definition

```c
static void
apply_handle_tuple_routing(ApplyExecutionData *edata,
						   TupleTableSlot *remoteslot,
						   LogicalRepTupleData *newtup,
						   CmdType operation)
```
## Detailed Description
This function implements the core logic for applying DML operations (INSERT, UPDATE, DELETE) on partitioned tables in logical replication. It handles the complex process of:

1. **Partition Discovery**: Uses tuple routing to find the correct partition for the incoming tuple
2. **Tuple Conversion**: Handles rowtype conversions between parent and child partitions when schemas differ
3. **Operation-Specific Logic**: 
   - For INSERT: Directly inserts into the target partition
   - For DELETE: Deletes from the appropriate partition
   - For UPDATE: Implements sophisticated logic that may result in either an in-place update or a cross-partition move (DELETE from old + INSERT into new)

The function sets up the necessary execution state including ModifyTableState and PartitionTupleRouting structures, then delegates to partition-specific internal functions for the actual DML operations.

For UPDATE operations, it performs additional validation to check if the updated tuple still satisfies the current partition's constraints. If not, it performs a cross-partition move by deleting the old tuple and inserting the new tuple into the correct partition.

## Parameters / Member Variables
- `*edata`: ApplyExecutionData structure containing execution context and target relation information
- `*remoteslot`: TupleTableSlot containing the incoming tuple from the remote publisher
- `*newtup`: LogicalRepTupleData containing new tuple data (used for UPDATE operations)
- `operation`: CmdType indicating the DML operation (CMD_INSERT, CMD_UPDATE, or CMD_DELETE)
## Dependencies
- Functions called/Symbols referenced:
  - [ExecSetupPartitionTupleRouting](../E/ExecSetupPartitionTupleRouting.md)
  - [ExecFindPartition](../E/ExecFindPartition.md)
  - [CheckSubscriptionRelkind](../C/CheckSubscriptionRelkind.md)
  - [ExecGetRootToChildMap](../E/ExecGetRootToChildMap.md)
  - [execute_attr_map_slot](../e/execute_attr_map_slot.md)
  - [apply_handle_insert_internal](apply_handle_insert_internal.md)
  - [apply_handle_delete_internal](apply_handle_delete_internal.md)
  - [FindReplTupleInLocalRel](../F/FindReplTupleInLocalRel.md)
  - [slot_modify_data](../s/slot_modify_data.md)
  - [ExecPartitionCheck](../E/ExecPartitionCheck.md)
  - [convert_tuples_by_name](../c/convert_tuples_by_name.md)
- Called from (representative examples):
  - [apply_handle_insert](apply_handle_insert.md)
  - [apply_handle_update](apply_handle_update.md)
  - [apply_handle_delete](apply_handle_delete.md)

## Notes and Other Information
- This function is crucial for logical replication of partitioned tables, ensuring data consistency across partition boundaries
- Handles complex scenarios like cross-partition UPDATEs where a tuple needs to move between partitions
- Includes comprehensive error handling and validation of partition constraints
- Uses memory context switching to manage temporary allocations during tuple processing
- The function validates that partitions have supported relation kinds for replication
- For cross-partition moves, it performs both DELETE and INSERT operations atomically within the same transaction

## Simplified Source

```c
static void
apply_handle_tuple_routing(ApplyExecutionData *edata,
                          TupleTableSlot *remoteslot,
                          LogicalRepTupleData *newtup,
                          CmdType operation)
{
    EState *estate = edata->estate;
    LogicalRepRelMapEntry *relmapentry = edata->targetRel;
    ResultRelInfo *relinfo = edata->targetRelInfo;
    Relation parentrel = relinfo->ri_RelationDesc;
    ModifyTableState *mtstate;
    PartitionTupleRouting *proute;
    ResultRelInfo *partrelinfo;
    Relation partrel;
    TupleTableSlot *remoteslot_part;
    LogicalRepRelMapEntry *part_entry = NULL;

    // Set up partition tuple routing infrastructure
    edata->mtstate = mtstate = makeNode(ModifyTableState);
    mtstate->ps.state = estate;
    mtstate->operation = operation;
    mtstate->resultRelInfo = relinfo;

    edata->proute = proute = ExecSetupPartitionTupleRouting(estate, parentrel);

    // Find the target partition for this tuple
    partrelinfo = ExecFindPartition(mtstate, relinfo, proute,
                                   remoteslot, estate);
    partrel = partrelinfo->ri_RelationDesc;

    // Validate partition is supported
    CheckSubscriptionRelkind(partrel->rd_rel->relkind,
                           get_namespace_name(RelationGetNamespace(partrel)),
                           RelationGetRelationName(partrel));

    // Convert tuple to partition's rowtype if needed
    remoteslot_part = partrelinfo->ri_PartitionTupleSlot;
    if (remoteslot_part == NULL)
        remoteslot_part = table_slot_create(partrel, &estate->es_tupleTable);

    TupleConversionMap *map = ExecGetRootToChildMap(partrelinfo, estate);
    if (map != NULL)
    {
        remoteslot_part = execute_attr_map_slot(map->attrMap, remoteslot,
                                              remoteslot_part);
    }
    else
    {
        remoteslot_part = ExecCopySlot(remoteslot_part, remoteslot);
        slot_getallattrs(remoteslot_part);
    }

    // Handle operation-specific logic
    switch (operation)
    {
        case CMD_INSERT:
            apply_handle_insert_internal(edata, partrelinfo, remoteslot_part);
            break;

        case CMD_DELETE:
            part_entry = logicalrep_partition_open(relmapentry, partrel,
                                                 map ? map->attrMap : NULL);
            apply_handle_delete_internal(edata, partrelinfo, remoteslot_part,
                                        part_entry->localindexoid);
            break;

        case CMD_UPDATE:
            part_entry = logicalrep_partition_open(relmapentry, partrel,
                                                 map ? map->attrMap : NULL);

            // Find existing tuple in partition
            TupleTableSlot *localslot;
            bool found = FindReplTupleInLocalRel(edata, partrel,
                                               &part_entry->remoterel,
                                               part_entry->localindexoid,
                                               remoteslot_part, &localslot);
            if (!found)
            {
                elog(DEBUG1, "tuple to update not found in partition");
                return;
            }

            // Apply update to create new tuple
            slot_modify_data(remoteslot_part, localslot, part_entry, newtup);

            // Check if updated tuple still belongs to this partition
            if (!partrel->rd_rel->relispartition ||
                ExecPartitionCheck(partrelinfo, remoteslot_part, estate, false))
            {
                // Simple update within same partition
                EPQState epqstate;
                EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1, NIL);
                EvalPlanQualSetSlot(&epqstate, remoteslot_part);
                TargetPrivilegesCheck(partrelinfo->ri_RelationDesc, ACL_UPDATE);
                ExecSimpleRelationUpdate(partrelinfo, estate, &epqstate,
                                       localslot, remoteslot_part);
                EvalPlanQualEnd(&epqstate);
            }
            else
            {
                // Cross-partition move: delete old + insert new
                apply_handle_delete_internal(edata, partrelinfo, localslot,
                                           part_entry->localindexoid);

                // Find new partition and insert there
                ResultRelInfo *new_partrelinfo = ExecFindPartition(mtstate, relinfo,
                                                                  proute, remoteslot,
                                                                  estate);
                apply_handle_insert_internal(edata, new_partrelinfo, remoteslot_part);
            }
            break;

        default:
            elog(ERROR, "unrecognized CmdType: %d", (int) operation);
    }
}
```