# ExecInsert

## Location
[src/backend/executor/nodeModifyTable.c:779-1243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L779-L1243)

## Overview
Handles the insertion of a single tuple into a table (or partition thereof) and its associated indexes, supporting complex scenarios like foreign tables, batch inserts, ON CONFLICT handling, and RETURNING clauses.

## Definition

```c
static TupleTableSlot *
ExecInsert(ModifyTableContext *context,
		   ResultRelInfo *resultRelInfo,
		   TupleTableSlot *slot,
		   bool canSetTag,
		   TupleTableSlot **inserted_tuple,
		   ResultRelInfo **insert_destrel)
```
## Detailed Description
ExecInsert is the core function responsible for inserting tuples in PostgreSQL's executor. It handles multiple insertion scenarios:

1. **Partition routing**: For partitioned tables, it finds the appropriate leaf partition to insert into
2. **Foreign table handling**: Delegates to FDW routines for foreign tables, including batch insertion support
3. **Regular table insertion**: Performs standard heap insertion with index maintenance
4. **Constraint validation**: Validates RLS policies, CHECK constraints, and partition constraints
5. **Conflict resolution**: Implements INSERT ... ON CONFLICT DO NOTHING/UPDATE logic using speculative insertion
6. **Trigger processing**: Executes BEFORE ROW, INSTEAD OF, and AFTER ROW triggers
7. **Generated columns**: Computes stored generated columns before insertion

The function supports batching for FDWs that can handle multiple rows efficiently. For ON CONFLICT scenarios, it uses speculative insertion to minimize rollback overhead when conflicts occur.

## Parameters / Member Variables
- : ModifyTableContext containing execution state and plan information
- : Information about the target relation for insertion  
- : TupleTableSlot containing the tuple values to be inserted
- : Whether the command tag counter should be incremented
- : Output parameter returning the effectively inserted tuple
- : Output parameter returning the relation where insertion occurred

## Dependencies
- Functions called/Symbols referenced:
  - [ExecPrepareTupleRouting](ExecPrepareTupleRouting.md) (partition routing)
  - [ExecMaterializeSlot](ExecMaterializeSlot.md) (tuple materialization)
  - [ExecBRInsertTriggers](ExecBRInsertTriggers.md)/ExecIRInsertTriggers/ExecARInsertTriggers (trigger handling)
  - [ExecComputeStoredGenerated](ExecComputeStoredGenerated.md) (generated columns)
  - [ExecBatchInsert](ExecBatchInsert.md) (FDW batch processing)
  - [ExecCheckIndexConstraints](ExecCheckIndexConstraints.md) (conflict detection)
  - [ExecOnConflictUpdate](ExecOnConflictUpdate.md) (ON CONFLICT DO UPDATE)
  - [table_tuple_insert](../t/table_tuple_insert.md)/table_tuple_insert_speculative (heap insertion)
  - [ExecInsertIndexTuples](ExecInsertIndexTuples.md) (index maintenance)
  - [ExecProcessReturning](ExecProcessReturning.md) (RETURNING clause processing)
- Called from (representative examples):
  - [ExecModifyTable](ExecModifyTable.md) (main INSERT execution)
  - [ExecMergeNotMatched](ExecMergeNotMatched.md) (MERGE statement INSERT actions)
  - [ExecCrossPartitionUpdate](ExecCrossPartitionUpdate.md) (partition key updates)

## Notes and Other Information
- The function may change the active tuple conversion map in mtstate->mt_transition_capture, requiring callers to save the previous value
- For FDW batch insertion, tuples are accumulated in ri_Slots until the batch size is reached
- Speculative insertion is used for ON CONFLICT to avoid expensive rollbacks on conflicts
- The function handles both regular and cross-partition insertions (when a tuple is moved between partitions during UPDATE)
- Memory contexts are carefully managed, especially for batch operations to avoid excessive memory usage
- The function returns NULL for "do nothing" cases or when batching (actual insertion is deferred)

## Simplified Source

```c
static TupleTableSlot *
ExecInsert(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
           TupleTableSlot *slot, bool canSetTag,
           TupleTableSlot **inserted_tuple, ResultRelInfo **insert_destrel)
{
    ModifyTableState *mtstate = context->mtstate;
    EState *estate = context->estate;
    Relation resultRelationDesc;
    TupleTableSlot *result = NULL;
    List *recheckIndexes = NIL;

    // Handle partition routing if needed
    if (mtstate->mt_partition_tuple_routing) {
        ResultRelInfo *partRelInfo;
        slot = ExecPrepareTupleRouting(mtstate, estate,
                                       mtstate->mt_partition_tuple_routing,
                                       resultRelInfo, slot, &partRelInfo);
        resultRelInfo = partRelInfo;
    }

    ExecMaterializeSlot(slot);
    resultRelationDesc = resultRelInfo->ri_RelationDesc;

    // Open indexes if needed
    if (resultRelationDesc->rd_rel->relhasindex &&
        resultRelInfo->ri_IndexRelationDescs == NULL)
        ExecOpenIndices(resultRelInfo, node->onConflictAction != ONCONFLICT_NONE);

    // Fire BEFORE ROW INSERT triggers
    if (resultRelInfo->ri_TrigDesc &&
        resultRelInfo->ri_TrigDesc->trig_insert_before_row) {
        if (estate->es_insert_pending_result_relations != NIL)
            ExecPendingInserts(estate);
        if (!ExecBRInsertTriggers(estate, resultRelInfo, slot))
            return NULL;  // "do nothing"
    }

    // Handle INSTEAD OF triggers or FDW/regular insertion
    if (resultRelInfo->ri_TrigDesc &&
        resultRelInfo->ri_TrigDesc->trig_insert_instead_row) {
        if (!ExecIRInsertTriggers(estate, resultRelInfo, slot))
            return NULL;
    }
    else if (resultRelInfo->ri_FdwRoutine) {
        // Foreign table insertion (simplified - removed batch logic)
        slot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

        if (resultRelationDesc->rd_att->constr &&
            resultRelationDesc->rd_att->constr->has_generated_stored)
            ExecComputeStoredGenerated(resultRelInfo, estate, slot, CMD_INSERT);

        slot = resultRelInfo->ri_FdwRoutine->ExecForeignInsert(estate, resultRelInfo,
                                                               slot, context->planSlot);
        if (slot == NULL)
            return NULL;
    }
    else {
        // Regular table insertion
        slot->tts_tableOid = RelationGetRelid(resultRelationDesc);

        // Compute generated columns
        if (resultRelationDesc->rd_att->constr &&
            resultRelationDesc->rd_att->constr->has_generated_stored)
            ExecComputeStoredGenerated(resultRelInfo, estate, slot, CMD_INSERT);

        // Check RLS policies and constraints
        if (resultRelInfo->ri_WithCheckOptions != NIL)
            ExecWithCheckOptions(WCO_RLS_INSERT_CHECK, resultRelInfo, slot, estate);
        if (resultRelationDesc->rd_att->constr)
            ExecConstraints(resultRelInfo, slot, estate);

        // Handle ON CONFLICT logic (simplified)
        if (node->onConflictAction != ONCONFLICT_NONE && resultRelInfo->ri_NumIndices > 0) {
            // Simplified conflict handling - removed speculative insertion details
            ItemPointerData conflictTid;
            if (!ExecCheckIndexConstraints(resultRelInfo, slot, estate,
                                           &conflictTid, resultRelInfo->ri_onConflictArbiterIndexes)) {
                if (node->onConflictAction == ONCONFLICT_UPDATE) {
                    TupleTableSlot *returning = NULL;
                    if (ExecOnConflictUpdate(context, resultRelInfo, &conflictTid,
                                             slot, canSetTag, &returning)) {
                        InstrCountTuples2(&mtstate->ps, 1);
                        return returning;
                    }
                } else {
                    // DO NOTHING case
                    InstrCountTuples2(&mtstate->ps, 1);
                    return NULL;
                }
            }
        }

        // Insert tuple and indexes
        table_tuple_insert(resultRelationDesc, slot, estate->es_output_cid, 0, NULL);
        if (resultRelInfo->ri_NumIndices > 0)
            recheckIndexes = ExecInsertIndexTuples(resultRelInfo, slot, estate,
                                                   false, false, NULL, NIL, false);
    }

    if (canSetTag)
        (estate->es_processed)++;

    // Fire AFTER ROW INSERT triggers
    ExecARInsertTriggers(estate, resultRelInfo, slot, recheckIndexes,
                         mtstate->mt_transition_capture);

    list_free(recheckIndexes);

    // Process RETURNING clause
    if (resultRelInfo->ri_projectReturning)
        result = ExecProcessReturning(resultRelInfo, slot, context->planSlot);

    if (inserted_tuple)
        *inserted_tuple = slot;
    if (insert_destrel)
        *insert_destrel = resultRelInfo;

    return result;
}
```