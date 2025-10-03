# ExecDelete

## Location
[src/backend/executor/nodeModifyTable.c:1449-1762](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L1449-L1762)

## Overview
Executes the deletion of a tuple from a table, handling various scenarios including foreign tables, triggers, concurrent modifications, and RETURNING clauses.

## Definition

```c
static TupleTableSlot *
ExecDelete(ModifyTableContext *context,
		   ResultRelInfo *resultRelInfo,
		   ItemPointer tupleid,
		   HeapTuple oldtuple,
		   bool processReturning,
		   bool changingPart,
		   bool canSetTag,
		   TM_Result *tmresult,
		   bool *tupleDeleted,
		   TupleTableSlot **epqreturnslot)
```
## Detailed Description
ExecDelete is the core function for executing DELETE operations in PostgreSQL. It handles the complete deletion workflow including:

1. **Preparation Phase**: Calls ExecDeletePrologue to handle BEFORE triggers and preliminary checks
2. **Trigger Handling**: Processes INSTEAD OF triggers for views and foreign table triggers
3. **Physical Deletion**: For regular tables, performs the actual tuple deletion via ExecDeleteAct
4. **Concurrency Control**: Handles concurrent modifications using EPQ (EvalPlanQual) mechanism
5. **Cleanup Phase**: Calls ExecDeleteEpilogue for AFTER triggers and cleanup
6. **RETURNING Processing**: Generates RETURNING clause results if requested

The function supports multiple deletion scenarios:
- Regular table deletion using tuple ID
- View deletion through INSTEAD OF triggers using old tuple data
- Foreign table deletion delegated to FDW routines
- Cross-partition updates where deletion is part of tuple movement

## Parameters / Member Variables
- `*context`: ModifyTableContext containing execution state and metadata
- `*resultRelInfo`: Information about the target relation for deletion
- `tupleid`: ItemPointer identifying the tuple to delete (invalid for foreign tables and views)
- `oldtuple`: HeapTuple containing tuple data (used for triggers, NULL for regular table deletion)
- `processReturning`: Boolean indicating whether to process RETURNING clause
- `changingPart`: Boolean indicating if deletion is part of cross-partition update
- `canSetTag`: Boolean controlling whether to increment processed tuple count
- `*tmresult`: Output parameter receiving the tuple modification result
- `*tupleDeleted`: Output parameter indicating whether deletion actually occurred
- `**epqreturnslot`: Output parameter for returning updated tuple from EPQ evaluation
## Dependencies
- Functions called/Symbols referenced:
  - [ExecDeletePrologue](ExecDeletePrologue.md) (preparation and BEFORE triggers)
  - [ExecDeleteAct](ExecDeleteAct.md) (physical deletion operation)
  - [ExecDeleteEpilogue](ExecDeleteEpilogue.md) (cleanup and AFTER triggers)
  - [ExecIRDeleteTriggers](ExecIRDeleteTriggers.md) (INSTEAD OF row delete triggers)
  - [ExecGetReturningSlot](ExecGetReturningSlot.md) (RETURNING slot management)
  - [EvalPlanQual](EvalPlanQual.md)/EvalPlanQualBegin (concurrency control)
  - [ExecProcessReturning](ExecProcessReturning.md) (RETURNING clause processing)
- Called from:
  - [ExecCrossPartitionUpdate](ExecCrossPartitionUpdate.md) (cross-partition tuple movement)
  - [ExecModifyTable](ExecModifyTable.md) (main modify table execution)

## Notes and Other Information
- Returns TupleTableSlot containing RETURNING results, or NULL if no RETURNING clause
- Handles complex concurrency scenarios including TM_Updated, TM_SelfModified, and TM_Deleted cases
- Uses EPQ mechanism to handle concurrent updates during deletion
- For foreign tables, delegates actual deletion to FDW's ExecForeignDelete routine
- Index tuple cleanup is deferred to VACUUM rather than being done immediately
- Supports serializable transaction isolation through snapshot checks
- Part of PostgreSQL's executor framework for DML operations

## Simplified Source

```c
static TupleTableSlot *
ExecDelete(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
           ItemPointer tupleid, HeapTuple oldtuple, bool processReturning,
           bool changingPart, bool canSetTag, TM_Result *tmresult,
           bool *tupleDeleted, TupleTableSlot **epqreturnslot) {
    EState *estate = context->estate;
    Relation resultRelationDesc = resultRelInfo->ri_RelationDesc;
    TupleTableSlot *slot = NULL;
    TM_Result result;

    if (tupleDeleted)
        *tupleDeleted = false;

    // Preparation phase: BEFORE triggers and checks
    if (!ExecDeletePrologue(context, resultRelInfo, tupleid, oldtuple,
                           epqreturnslot, tmresult))
        return NULL;

    // Handle INSTEAD OF triggers for views
    if (resultRelInfo->ri_TrigDesc &&
        resultRelInfo->ri_TrigDesc->trig_delete_instead_row) {
        Assert(oldtuple != NULL);
        bool dodelete = ExecIRDeleteTriggers(estate, resultRelInfo, oldtuple);
        if (!dodelete)
            return NULL;
    }
    // Handle foreign table deletion
    else if (resultRelInfo->ri_FdwRoutine) {
        slot = ExecGetReturningSlot(estate, resultRelInfo);
        slot = resultRelInfo->ri_FdwRoutine->ExecForeignDelete(estate, resultRelInfo,
                                                              slot, context->planSlot);
        if (slot == NULL)
            return NULL;

        // Initialize tableOid for RETURNING expressions
        if (TTS_EMPTY(slot))
            ExecStoreAllNullTuple(slot);
        slot->tts_tableOid = RelationGetRelid(resultRelationDesc);
    }
    // Handle regular table deletion
    else {
ldelete:
        result = ExecDeleteAct(context, resultRelInfo, tupleid, changingPart);
        if (tmresult)
            *tmresult = result;

        switch (result) {
            case TM_SelfModified:
                // Handle concurrent self-modification
                if (context->tmfd.cmax != estate->es_output_cid)
                    ereport(ERROR, (errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
                                   errmsg("tuple to be deleted was already modified by an operation triggered by the current command")));
                return NULL;

            case TM_Ok:
                break;

            case TM_Updated:
                // Handle concurrent update using EPQ
                if (IsolationUsesXactSnapshot())
                    ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                                   errmsg("could not serialize access due to concurrent update")));

                EvalPlanQualBegin(context->epqstate);
                TupleTableSlot *inputslot = EvalPlanQualSlot(context->epqstate, resultRelationDesc,
                                                            resultRelInfo->ri_RangeTableIndex);

                result = table_tuple_lock(resultRelationDesc, tupleid, estate->es_snapshot,
                                        inputslot, estate->es_output_cid, LockTupleExclusive,
                                        LockWaitBlock, TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
                                        &context->tmfd);

                switch (result) {
                    case TM_Ok:
                        Assert(context->tmfd.traversed);
                        TupleTableSlot *epqslot = EvalPlanQual(context->epqstate, resultRelationDesc,
                                                              resultRelInfo->ri_RangeTableIndex, inputslot);
                        if (TupIsNull(epqslot))
                            return NULL;

                        if (epqreturnslot) {
                            *epqreturnslot = epqslot;
                            return NULL;
                        } else
                            goto ldelete;

                    case TM_SelfModified:
                        if (context->tmfd.cmax != estate->es_output_cid)
                            ereport(ERROR, (errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
                                           errmsg("tuple to be deleted was already modified by an operation triggered by the current command")));
                        return NULL;

                    case TM_Deleted:
                        return NULL;

                    default:
                        elog(ERROR, "unexpected table_tuple_lock status: %u", result);
                        return NULL;
                }
                break;

            case TM_Deleted:
                if (IsolationUsesXactSnapshot())
                    ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                                   errmsg("could not serialize access due to concurrent delete")));
                return NULL;

            default:
                elog(ERROR, "unrecognized table_tuple_delete status: %u", result);
                return NULL;
        }
    }

    // Update processed count and mark as deleted
    if (canSetTag)
        (estate->es_processed)++;

    if (tupleDeleted)
        *tupleDeleted = true;

    // Cleanup phase: AFTER triggers
    ExecDeleteEpilogue(context, resultRelInfo, tupleid, oldtuple, changingPart);

    // Process RETURNING clause if requested
    if (processReturning && resultRelInfo->ri_projectReturning) {
        TupleTableSlot *rslot;

        if (resultRelInfo->ri_FdwRoutine) {
            Assert(!TupIsNull(slot));
        } else {
            slot = ExecGetReturningSlot(estate, resultRelInfo);
            if (oldtuple != NULL) {
                ExecForceStoreHeapTuple(oldtuple, slot, false);
            } else {
                if (!table_tuple_fetch_row_version(resultRelationDesc, tupleid,
                                                  SnapshotAny, slot))
                    elog(ERROR, "failed to fetch deleted tuple for DELETE RETURNING");
            }
        }

        rslot = ExecProcessReturning(resultRelInfo, slot, context->planSlot);
        ExecMaterializeSlot(rslot);
        ExecClearTuple(slot);
        return rslot;
    }

    return NULL;
}
```