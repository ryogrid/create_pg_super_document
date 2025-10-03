# ExecUpdate

## Location
[src/backend/executor/nodeModifyTable.c:2292-2543](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L2292-L2543)

## Overview
ExecUpdate is the main function responsible for executing UPDATE operations in PostgreSQL, handling various update scenarios including regular table updates, foreign table updates, view updates, and cross-partition updates.

## Definition

```c
static TupleTableSlot *
ExecUpdate(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
		   ItemPointer tupleid, HeapTuple oldtuple, TupleTableSlot *slot,
		   bool canSetTag)
```
## Detailed Description
ExecUpdate orchestrates the complete UPDATE operation workflow in PostgreSQL. The function handles multiple update scenarios based on the target relation type:

1. **Bootstrap Mode Validation**: Prevents updates during database bootstrap
2. **Update Preparation**: Calls ExecUpdatePrologue to handle BEFORE ROW triggers and validation
3. **INSTEAD OF Trigger Handling**: For view updates, executes INSTEAD OF ROW UPDATE triggers
4. **Foreign Table Updates**: Delegates to FDW-specific update routines for foreign tables
5. **Regular Table Updates**: Performs the core update logic including:
   - Tuple locking and concurrency control via ExecUpdateAct
   - EvalPlanQual (EPQ) processing for concurrent updates
   - Cross-partition update handling
   - Tuple versioning and conflict resolution
6. **Post-Update Processing**: Calls ExecUpdateEpilogue for index maintenance and AFTER triggers
7. **RETURNING Clause Processing**: Handles RETURNING expressions if present

The function implements PostgreSQL's Multi-Version Concurrency Control (MVCC) semantics and handles various tuple modification states (TM_Ok, TM_Updated, TM_Deleted, TM_SelfModified).

## Parameters / Member Variables
- `*context`: ModifyTableContext containing execution state, EPQ state, and command metadata
- `*resultRelInfo`: ResultRelInfo for the target relation being updated
- `tupleid`: ItemPointer identifying the tuple to update (invalid for foreign tables and view triggers)
- `oldtuple`: HeapTuple containing original data (for view triggers and foreign tables)
- `*slot`: TupleTableSlot containing the new tuple values
- `canSetTag`: Boolean indicating whether the processed tuple count can be incremented
## Dependencies
- Functions called/Symbols referenced:
  - [ExecUpdatePrologue](ExecUpdatePrologue.md)
  - [ExecIRUpdateTriggers](ExecIRUpdateTriggers.md)  
  - [ExecUpdatePrepareSlot](ExecUpdatePrepareSlot.md)
  - [ExecUpdateAct](ExecUpdateAct.md)
  - [ExecUpdateEpilogue](ExecUpdateEpilogue.md)
  - [EvalPlanQualSlot](EvalPlanQualSlot.md)
  - [EvalPlanQual](EvalPlanQual.md)
  - [ExecInitUpdateProjection](ExecInitUpdateProjection.md)
  - [ExecGetUpdateNewTuple](ExecGetUpdateNewTuple.md)
  - [ExecProcessReturning](ExecProcessReturning.md)
  - [table_tuple_lock](../t/table_tuple_lock.md)
  - [table_tuple_fetch_row_version](../t/table_tuple_fetch_row_version.md)
  - IsolationUsesXactSnapshot
- Called from (representative examples):
  - [ExecOnConflictUpdate](ExecOnConflictUpdate.md) (src/backend/executor/nodeModifyTable.c:2746)
  - [ExecModifyTable](ExecModifyTable.md) (src/backend/executor/nodeModifyTable.c:4316)

## Notes and Other Information
- The function is static and only used within nodeModifyTable.c
- Cross-partition updates are transparently handled and may return early with special RETURNING slot processing
- [EvalPlanQual](EvalPlanQual.md) processing ensures snapshot isolation by re-evaluating plan conditions for concurrently modified tuples
- The function includes comprehensive error handling for concurrent modifications and serialization failures
- For foreign tables, the FDW is responsible for determining which row to update using plan slot data
- INSTEAD OF triggers are used for view updates where the view itself is not directly updatable
- The redo_act label enables retry logic when EPQ determines a new tuple version should be processed
- Bootstrap mode restrictions prevent infinite update loops during database initialization

## Simplified Source

```c
static TupleTableSlot *
ExecUpdate(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
           ItemPointer tupleid, HeapTuple oldtuple, TupleTableSlot *slot,
           bool canSetTag)
{
    EState *estate = context->estate;
    Relation resultRelationDesc = resultRelInfo->ri_RelationDesc;
    UpdateContext updateCxt = {0};
    TM_Result result;

    // Prevent updates during bootstrap
    if (IsBootstrapProcessingMode())
        elog(ERROR, "cannot UPDATE during bootstrap");

    // Handle BEFORE ROW triggers and validation
    if (!ExecUpdatePrologue(context, resultRelInfo, tupleid, oldtuple, slot, NULL))
        return NULL;

    // Handle INSTEAD OF ROW UPDATE triggers for views
    if (resultRelInfo->ri_TrigDesc &&
        resultRelInfo->ri_TrigDesc->trig_update_instead_row) {
        if (!ExecIRUpdateTriggers(estate, resultRelInfo, oldtuple, slot))
            return NULL;  // "do nothing"
    }
    else if (resultRelInfo->ri_FdwRoutine) {
        // Foreign table update
        ExecUpdatePrepareSlot(resultRelInfo, slot, estate);
        slot = resultRelInfo->ri_FdwRoutine->ExecForeignUpdate(estate,
                                                               resultRelInfo,
                                                               slot,
                                                               context->planSlot);
        if (slot == NULL)
            return NULL;
        slot->tts_tableOid = RelationGetRelid(resultRelationDesc);
    }
    else {
        // Regular table update with concurrency control
        ItemPointerData lockedtid;

redo_act:
        lockedtid = *tupleid;
        result = ExecUpdateAct(context, resultRelInfo, tupleid, oldtuple, slot,
                               canSetTag, &updateCxt);

        // Handle cross-partition updates
        if (updateCxt.crossPartUpdate)
            return context->cpUpdateReturningSlot;

        switch (result)
        {
            case TM_SelfModified:
                // Check for concurrent modifications by same transaction
                if (context->tmfd.cmax != estate->es_output_cid)
                    ereport(ERROR, /* detailed error handling */);
                return NULL;

            case TM_Ok:
                break;

            case TM_Updated:
                // Handle concurrent updates with EvalPlanQual
                if (IsolationUsesXactSnapshot())
                    ereport(ERROR, /* serialization failure */);

                // Re-evaluate plan for updated tuple
                TupleTableSlot *inputslot = EvalPlanQualSlot(context->epqstate,
                                                             resultRelationDesc,
                                                             resultRelInfo->ri_RangeTableIndex);
                // Lock and re-fetch the updated tuple
                result = table_tuple_lock(resultRelationDesc, tupleid, /*...*/);

                if (result == TM_Ok) {
                    TupleTableSlot *epqslot = EvalPlanQual(context->epqstate, /*...*/);
                    if (TupIsNull(epqslot))
                        return NULL;

                    // Prepare for retry with updated tuple
                    if (unlikely(!resultRelInfo->ri_projectNewInfoValid))
                        ExecInitUpdateProjection(context->mtstate, resultRelInfo);

                    TupleTableSlot *oldSlot = resultRelInfo->ri_oldTupleSlot;
                    table_tuple_fetch_row_version(resultRelationDesc, tupleid,
                                                  SnapshotAny, oldSlot);
                    slot = ExecGetUpdateNewTuple(resultRelInfo, epqslot, oldSlot);
                    goto redo_act;
                }
                return NULL;

            case TM_Deleted:
                if (IsolationUsesXactSnapshot())
                    ereport(ERROR, /* serialization failure */);
                return NULL;

            default:
                elog(ERROR, "unrecognized table_tuple_update status: %u", result);
        }
    }

    if (canSetTag)
        (estate->es_processed)++;

    // Handle index updates and AFTER triggers
    ExecUpdateEpilogue(context, &updateCxt, resultRelInfo, tupleid, oldtuple, slot);

    // Process RETURNING clause if present
    if (resultRelInfo->ri_projectReturning)
        return ExecProcessReturning(resultRelInfo, slot, context->planSlot);

    return NULL;
}
```