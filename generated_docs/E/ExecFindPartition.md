# ExecFindPartition

## Location
[src/backend/executor/execPartition.c:262-494](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execPartition.c#L262-L494)

## Overview
Returns the ResultRelInfo for the leaf partition that a tuple should belong to, performing partition key evaluation and traversing the partition hierarchy to locate the appropriate destination partition.

## Definition

```c
ResultRelInfo *
ExecFindPartition(ModifyTableState *mtstate,
				  ResultRelInfo *rootResultRelInfo,
				  PartitionTupleRouting *proute,
				  TupleTableSlot *slot, EState *estate)
```
## Detailed Description
This function implements the core partition routing algorithm for PostgreSQL's partitioned tables. It evaluates partition key expressions against the input tuple and traverses the partition hierarchy from root to leaf, handling both single-level and multi-level partitioning schemes. The function employs lazy initialization, creating ResultRelInfo structures only when a partition is first accessed. It also handles tuple format conversion when moving between partitioning levels that have different tuple descriptors, and performs partition constraint validation for default partitions to ensure data consistency.

The algorithm starts at the root partitioned table and iteratively evaluates partition keys to determine the target child partition. For sub-partitioned tables, it recursively descends through the hierarchy until reaching a leaf partition. The function reuses existing ResultRelInfo structures when possible and creates new ones as needed, optimizing memory usage and performance.

## Parameters / Member Variables
- : ModifyTableState containing information about the modify operation and available ResultRelInfo structures
- : The ResultRelInfo for the root relation named in the query
- : PartitionTupleRouting structure containing partition dispatch information and cached ResultRelInfo structures
- : TupleTableSlot containing the tuple to be routed to its appropriate partition
- : Executor state providing expression evaluation context and memory management

## Dependencies
- Functions called/Symbols referenced:
  - GetPerTupleExprContext
  - GetPerTupleMemoryContext
  - [ExecPartitionCheck](ExecPartitionCheck.md)
  - [FormPartitionKeyDatum](../F/FormPartitionKeyDatum.md)
  - [get_partition_for_tuple](../g/get_partition_for_tuple.md)
  - [ExecBuildSlotPartitionKeyDescription](ExecBuildSlotPartitionKeyDescription.md)
  - [ExecLookupResultRelByOid](ExecLookupResultRelByOid.md)
  - [CheckValidResultRel](../C/CheckValidResultRel.md)
  - [ExecInitRoutingInfo](ExecInitRoutingInfo.md)
  - [ExecInitPartitionInfo](ExecInitPartitionInfo.md)
  - [ExecInitPartitionDispatchInfo](ExecInitPartitionDispatchInfo.md)
  - [ExecGetRootToChildMap](ExecGetRootToChildMap.md)
  - [execute_attr_map_slot](../e/execute_attr_map_slot.md)
  - [ExecClearTuple](ExecClearTuple.md)
- Called from (representative examples):
  - [CopyFrom](../C/CopyFrom.md) (in copyfrom.c:1055)
  - [ExecPrepareTupleRouting](ExecPrepareTupleRouting.md) (in nodeModifyTable.c:3910)
  - [apply_handle_tuple_routing](../a/apply_handle_tuple_routing.md) (in worker.c:2942, 3097)

## Notes and Other Information
The function uses per-tuple memory context to avoid memory leaks during partition key evaluation. It handles tuple format conversion when traversing between partitioning levels with different tuple descriptors. Special attention is paid to default partitions, which require constraint validation to ensure the tuple actually belongs there. The function raises appropriate errors if no suitable partition is found or if the target partition is not valid for INSERT operations.

## Simplified Source

```c
ResultRelInfo *
ExecFindPartition(ModifyTableState *mtstate, ResultRelInfo *rootResultRelInfo,
                 PartitionTupleRouting *proute, TupleTableSlot *slot, EState *estate)
{
    PartitionDispatch *pd = proute->partition_dispatch_info;
    Datum values[PARTITION_MAX_KEYS];
    bool isnull[PARTITION_MAX_KEYS];
    ExprContext *ecxt = GetPerTupleExprContext(estate);
    TupleTableSlot *ecxt_scantuple_saved = ecxt->ecxt_scantuple;
    TupleTableSlot *rootslot = slot;
    TupleTableSlot *myslot = NULL;
    MemoryContext oldcxt;
    ResultRelInfo *rri = NULL;

    // Switch to per-tuple memory context
    oldcxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

    // Check root table's partition constraint if it's a partition
    if (rootResultRelInfo->ri_RelationDesc->rd_rel->relispartition)
        ExecPartitionCheck(rootResultRelInfo, slot, estate, true);

    // Start with root partitioned table and traverse hierarchy
    PartitionDispatch dispatch = pd[0];
    while (dispatch != NULL)
    {
        int partidx = -1;
        bool is_leaf;

        CHECK_FOR_INTERRUPTS();

        Relation rel = dispatch->reldesc;
        PartitionDesc partdesc = dispatch->partdesc;

        // Extract partition key from tuple
        ecxt->ecxt_scantuple = slot;
        FormPartitionKeyDatum(dispatch, slot, estate, values, isnull);

        // Find matching partition
        if (partdesc->nparts == 0 ||
            (partidx = get_partition_for_tuple(dispatch, values, isnull)) < 0)
        {
            // No matching partition found - error
            char *val_desc = ExecBuildSlotPartitionKeyDescription(rel, values, isnull, 64);
            ereport(ERROR, (errcode(ERRCODE_CHECK_VIOLATION),
                           errmsg("no partition of relation \"%s\" found for row",
                                  RelationGetRelationName(rel)),
                           val_desc ? errdetail("Partition key of the failing row contains %s.", val_desc) : 0));
        }

        is_leaf = partdesc->is_leaf[partidx];
        if (is_leaf)
        {
            // Found leaf partition - get or create ResultRelInfo
            if (likely(dispatch->indexes[partidx] >= 0))
            {
                // Already exists
                rri = proute->partitions[dispatch->indexes[partidx]];
            }
            else
            {
                // Look for existing or create new ResultRelInfo
                rri = ExecLookupResultRelByOid(mtstate, partdesc->oids[partidx], true, false);
                if (rri)
                {
                    CheckValidResultRel(rri, CMD_INSERT, NIL);
                    ExecInitRoutingInfo(mtstate, estate, proute, dispatch, rri, partidx, true);
                }
                else
                {
                    rri = ExecInitPartitionInfo(mtstate, estate, proute, dispatch,
                                              rootResultRelInfo, partidx);
                }
            }
            dispatch = NULL; // Exit loop
        }
        else
        {
            // Sub-partitioned table - continue traversing
            if (likely(dispatch->indexes[partidx] >= 0))
            {
                rri = proute->nonleaf_partitions[dispatch->indexes[partidx]];
                dispatch = pd[dispatch->indexes[partidx]];
            }
            else
            {
                // Create new PartitionDispatch for sub-partition
                PartitionDispatch subdispatch = ExecInitPartitionDispatchInfo(estate, proute,
                                                                            partdesc->oids[partidx],
                                                                            dispatch, partidx,
                                                                            mtstate->rootResultRelInfo);
                rri = proute->nonleaf_partitions[dispatch->indexes[partidx]];
                dispatch = subdispatch;
            }

            // Convert tuple format if needed for sub-partition
            if (dispatch->tupslot)
            {
                TupleTableSlot *tempslot = myslot;
                myslot = dispatch->tupslot;
                slot = execute_attr_map_slot(dispatch->tupmap, slot, myslot);
                if (tempslot != NULL)
                    ExecClearTuple(tempslot);
            }
        }

        // Validate default partition constraint if this is the default
        if (partidx == partdesc->boundinfo->default_index)
        {
            if (is_leaf)
            {
                TupleConversionMap *map = ExecGetRootToChildMap(rri, estate);
                slot = map ? execute_attr_map_slot(map->attrMap, rootslot, rri->ri_PartitionTupleSlot) : rootslot;
            }
            ExecPartitionCheck(rri, slot, estate, true);
        }
    }

    // Cleanup
    if (myslot != NULL)
        ExecClearTuple(myslot);
    ecxt->ecxt_scantuple = ecxt_scantuple_saved;
    MemoryContextSwitchTo(oldcxt);

    return rri;
}
```