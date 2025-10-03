# ExecModifyTable

## Location
[src/backend/executor/nodeModifyTable.c:3953-4372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L3953-L4372)

## Overview
The main execution function for ModifyTable plan nodes, processing INSERT, UPDATE, DELETE, and MERGE operations while handling triggers, tuple routing, and RETURNING clauses.

## Definition
```c
static TupleTableSlot *ExecModifyTable(PlanState *pstate)
```

## Detailed Description
ExecModifyTable is the core execution engine for data modification operations in PostgreSQL. It orchestrates the entire process of modifying table data, including firing BEFORE and AFTER triggers, handling partition tuple routing, managing foreign data wrapper direct modifications, processing RETURNING clauses, and executing batch inserts. The function operates in a loop, fetching tuples from its subplan and performing the appropriate modification operation based on the command type. It handles complex scenarios like MERGE operations with WHEN NOT MATCHED clauses, multi-table modifications with OID-based relation selection, and various tuple identification mechanisms for different relation types. The function ensures proper transaction semantics and trigger execution order while maintaining performance through optimizations like batch processing and direct FDW modifications.

## Parameters / Member Variables
- `pstate`: Pointer to PlanState (cast to ModifyTableState) containing the execution state, plan information, and modification context

## Dependencies
- Functions called/Symbols referenced:
  - [fireBSTriggers](../f/fireBSTriggers.md), fireASTriggers
  - [ExecProcNode](ExecProcNode.md), TupIsNull
  - [ExecMergeNotMatched](ExecMergeNotMatched.md), ExecMerge
  - [ExecInsert](ExecInsert.md), ExecUpdate, ExecDelete
  - [ExecGetInsertNewTuple](ExecGetInsertNewTuple.md), ExecGetUpdateNewTuple
  - [ExecInitInsertProjection](ExecInitInsertProjection.md), ExecInitUpdateProjection
  - [ExecProcessReturning](ExecProcessReturning.md), ExecPendingInserts
  - [ExecLookupResultRelByOid](ExecLookupResultRelByOid.md), ExecGetJunkAttribute
  - EvalPlanQualSetSlot, ResetPerTupleExprContext
  - [table_tuple_fetch_row_version](../t/table_tuple_fetch_row_version.md), LockTuple, UnlockTuple
  - Various relation kind constants (RELKIND_RELATION, RELKIND_VIEW, etc.)
  - [Command](../C/Command.md) type constants (CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_MERGE)
- Called from (representative examples):
  - [ExecInitModifyTable](ExecInitModifyTable.md) (at src/backend/executor/nodeModifyTable.c:4443)

## Notes and Other Information
- Returns TupleTableSlot containing RETURNING results, or NULL when processing is complete
- Prevents execution during EvalPlanQual operations to avoid corruption
- Handles multi-relation modifications using junk columns for relation OID identification
- Supports foreign data wrapper direct modifications for improved performance
- Manages tuple identity information differently based on relation type (TID for heap tables, wholerow for others)
- Implements special handling for MERGE operations including deferred WHEN NOT MATCHED processing
- Processes batch inserts at the end of execution for optimal performance
- Maintains proper trigger firing order: BEFORE triggers before modifications, AFTER triggers after all processing
- Located in src/backend/executor/nodeModifyTable.c:3953-4372

## Simplified Source

```c
static TupleTableSlot *ExecModifyTable(PlanState *pstate)
{
    ModifyTableState *node = castNode(ModifyTableState, pstate);
    ModifyTableContext context;
    EState *estate = node->ps.state;
    CmdType operation = node->operation;

    CHECK_FOR_INTERRUPTS();

    // Prevent execution during EvalPlanQual operations
    if (estate->es_epq_active != NULL)
        elog(ERROR, "ModifyTable should not be called during EvalPlanQual");

    // Early return if already completed processing
    if (node->mt_done)
        return NULL;

    // Fire BEFORE STATEMENT triggers on first call
    if (node->fireBSTriggers)
    {
        fireBSTriggers(node);
        node->fireBSTriggers = false;
    }

    // Setup execution context
    ResultRelInfo *resultRelInfo = node->resultRelInfo + node->mt_lastResultIndex;
    PlanState *subplanstate = outerPlanState(node);
    context.mtstate = node;
    context.epqstate = &node->mt_epqstate;
    context.estate = estate;

    // Main processing loop
    for (;;)
    {
        ResetPerTupleExprContext(estate);
        ResetExprContext(pstate->ps_ExprContext);

        // Handle pending MERGE WHEN NOT MATCHED actions
        if (node->mt_merge_pending_not_matched != NULL)
        {
            TupleTableSlot *slot = ExecMergeNotMatched(&context, node->resultRelInfo,
                                                       node->canSetTag);
            node->mt_merge_pending_not_matched = NULL;
            if (slot)
                return slot;
            continue;
        }

        // Get next tuple from subplan
        context.planSlot = ExecProcNode(subplanstate);
        if (TupIsNull(context.planSlot))
            break;

        // Select correct result relation for multi-table operations
        if (AttributeNumberIsValid(node->mt_resultOidAttno))
        {
            Oid resultoid = ExecGetJunkAttribute(context.planSlot, node->mt_resultOidAttno, &isNull);
            if (resultoid != node->mt_lastResultOid)
                resultRelInfo = ExecLookupResultRelByOid(node, resultoid, false, true);
        }

        // Handle FDW direct modifications
        if (resultRelInfo->ri_usesFdwDirectModify)
        {
            return ExecProcessReturning(resultRelInfo, NULL, context.planSlot);
        }

        // Extract row identity for UPDATE/DELETE/MERGE operations
        ItemPointer tupleid = NULL;
        HeapTuple oldtuple = NULL;
        if (operation == CMD_UPDATE || operation == CMD_DELETE || operation == CMD_MERGE)
        {
            // Extract TID or wholerow based on relation type
            // (simplified - detailed extraction logic omitted)
        }

        // Execute the appropriate modification operation
        TupleTableSlot *slot = NULL;
        switch (operation)
        {
            case CMD_INSERT:
                slot = ExecGetInsertNewTuple(resultRelInfo, context.planSlot);
                slot = ExecInsert(&context, resultRelInfo, slot, node->canSetTag, NULL, NULL);
                break;

            case CMD_UPDATE:
                slot = ExecGetUpdateNewTuple(resultRelInfo, context.planSlot, oldSlot);
                slot = ExecUpdate(&context, resultRelInfo, tupleid, oldtuple,
                                  slot, node->canSetTag);
                break;

            case CMD_DELETE:
                slot = ExecDelete(&context, resultRelInfo, tupleid, oldtuple,
                                  true, false, node->canSetTag, NULL, NULL, NULL);
                break;

            case CMD_MERGE:
                slot = ExecMerge(&context, resultRelInfo, tupleid, oldtuple, node->canSetTag);
                break;
        }

        // Return RETURNING results if any
        if (slot)
            return slot;
    }

    // Complete any pending batch inserts
    if (estate->es_insert_pending_result_relations != NIL)
        ExecPendingInserts(estate);

    // Fire AFTER STATEMENT triggers
    fireASTriggers(node);
    node->mt_done = true;

    return NULL;
}
```