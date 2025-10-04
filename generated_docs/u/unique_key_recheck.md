# unique_key_recheck

## Location
[src/backend/commands/constraint.c:39-206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/constraint.c#L39-L206)

## Overview
A trigger function that performs deferred uniqueness and exclusion constraint checks for rows that may potentially violate deferrable unique or exclusion constraints.

## Definition
```c
Datum unique_key_recheck(PG_FUNCTION_ARGS)
```

## Detailed Description
The `unique_key_recheck` function is invoked as an AFTER ROW trigger for both INSERT and UPDATE operations on rows that have been recorded as potentially violating a deferrable unique or exclusion constraint. Despite its name suggesting only uniqueness checks, this function also handles deferred exclusion-constraint checks, making the name somewhat historical.

The function can be triggered in three scenarios:
- End-of-statement check
- Commit-time check  
- Check triggered by a SET CONSTRAINTS command

The function performs several key operations:
1. Validates that it is being called correctly as an AFTER ROW trigger
2. Retrieves the tuple data that was inserted or updated
3. Checks if the row is still live (not deleted within the same transaction)
4. Opens the associated constraint index with RowExclusiveLock
5. Forms the index values from the tuple data
6. Performs either uniqueness checking (via index_insert) or exclusion constraint checking
7. Cleans up resources and returns

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `fcinfo->context`: TriggerData structure with trigger-specific information

## Dependencies
- Functions called/Symbols referenced:
  - CALLED_AS_TRIGGER
  - TRIGGER_FIRED_AFTER
  - TRIGGER_FIRED_FOR_ROW  
  - TRIGGER_FIRED_BY_INSERT
  - TRIGGER_FIRED_BY_UPDATE
  - [table_slot_create](../t/table_slot_create.md)
  - [table_index_fetch_begin](../t/table_index_fetch_begin.md)
  - [table_index_fetch_tuple](../t/table_index_fetch_tuple.md)
  - [table_index_fetch_end](../t/table_index_fetch_end.md)
  - [index_open](../i/index_open.md)
  - [BuildIndexInfo](../B/BuildIndexInfo.md)
  - [CreateExecutorState](../C/CreateExecutorState.md)
  - GetPerTupleExprContext
  - [FormIndexDatum](../F/FormIndexDatum.md)
  - [index_insert](../i/index_insert.md)
  - [index_insert_cleanup](../i/index_insert_cleanup.md)
  - [check_exclusion_constraint](../c/check_exclusion_constraint.md)
  - [FreeExecutorState](../F/FreeExecutorState.md)
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
  - [index_close](../i/index_close.md)
- Called from:
  - No direct references found (invoked by PostgreSQL trigger system)

## Notes and Other Information
- The function name is somewhat historical - it now handles both unique and exclusion constraints
- Uses SnapshotSelf to check tuple visibility, allowing detection of rows deleted within the same transaction
- Handles HOT (Heap-Only Tuple) updates correctly by using the live child row for exclusion constraint checks
- Acquires RowExclusiveLock on the constraint index to protect against schema changes
- Creates an executor state only when needed (for expression evaluation or exclusion constraints)
- The function returns NULL on successful completion
- Located in src/backend/commands/constraint.c:39-206

## Simplified Source

```c
Datum
unique_key_recheck(PG_FUNCTION_ARGS)
{
    TriggerData *trigdata = (TriggerData *) fcinfo->context;
    ItemPointerData checktid;
    ItemPointerData tmptid;
    Relation indexRel;
    IndexInfo *indexInfo;
    EState *estate;
    TupleTableSlot *slot;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];

    // Validate trigger context
    if (!CALLED_AS_TRIGGER(fcinfo) ||
        !TRIGGER_FIRED_AFTER(trigdata->tg_event) ||
        !TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
        ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                       errmsg("function must be fired AFTER ROW")));

    // Get tuple ID from INSERT or UPDATE
    if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
        checktid = trigdata->tg_trigslot->tts_tid;
    else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
        checktid = trigdata->tg_newslot->tts_tid;
    else
        ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                       errmsg("function must be fired for INSERT or UPDATE")));

    slot = table_slot_create(trigdata->tg_relation, NULL);

    // Check if row is still live (skip if deleted in same transaction)
    tmptid = checktid;
    {
        IndexFetchTableData *scan = table_index_fetch_begin(trigdata->tg_relation);
        bool call_again = false;

        if (!table_index_fetch_tuple(scan, &tmptid, SnapshotSelf, slot, &call_again, NULL))
        {
            // Row is dead, skip check
            ExecDropSingleTupleTableSlot(slot);
            table_index_fetch_end(scan);
            return PointerGetDatum(NULL);
        }
        table_index_fetch_end(scan);
    }

    // Open constraint index and build index info
    indexRel = index_open(trigdata->tg_trigger->tgconstrindid, RowExclusiveLock);
    indexInfo = BuildIndexInfo(indexRel);

    // Create executor state if needed for expressions or exclusion constraints
    if (indexInfo->ii_Expressions != NIL || indexInfo->ii_ExclusionOps != NULL)
    {
        estate = CreateExecutorState();
        ExprContext *econtext = GetPerTupleExprContext(estate);
        econtext->ecxt_scantuple = slot;
    }
    else
        estate = NULL;

    // Form index values from tuple
    FormIndexDatum(indexInfo, slot, estate, values, isnull);

    // Perform appropriate constraint check
    if (indexInfo->ii_ExclusionOps == NULL)
    {
        // Uniqueness constraint check
        index_insert(indexRel, values, isnull, &checktid,
                     trigdata->tg_relation, UNIQUE_CHECK_EXISTING, false, indexInfo);
        index_insert_cleanup(indexRel, indexInfo);
    }
    else
    {
        // Exclusion constraint check
        check_exclusion_constraint(trigdata->tg_relation, indexRel, indexInfo,
                                   &tmptid, values, isnull, estate, false);
    }

    // Cleanup
    if (estate != NULL)
        FreeExecutorState(estate);
    ExecDropSingleTupleTableSlot(slot);
    index_close(indexRel, RowExclusiveLock);

    return PointerGetDatum(NULL);
}
```