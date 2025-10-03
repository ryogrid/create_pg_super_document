# ExecMergeMatched

## Location
[src/backend/executor/nodeModifyTable.c:2890-3400](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L2890-L3400)

## Overview
Handles execution of WHEN MATCHED and WHEN NOT MATCHED BY SOURCE actions in MERGE statements, including concurrent update detection and recovery logic.

## Definition

```c
static TupleTableSlot *
ExecMergeMatched(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
				 ItemPointer tupleid, HeapTuple oldtuple, bool canSetTag,
				 bool *matched)
```
## Detailed Description
ExecMergeMatched is responsible for executing the first qualifying WHEN MATCHED or WHEN NOT MATCHED BY SOURCE action in a MERGE statement. It handles complex scenarios involving concurrent modifications during MERGE execution:

1. **Action Selection**: Tests join conditions to determine whether to process WHEN MATCHED or WHEN NOT MATCHED BY SOURCE actions
2. **Concurrent Update Handling**: Detects and adapts to concurrent updates that may change match status during execution
3. **Action Execution**: Performs UPDATE, DELETE, or DO NOTHING operations based on the qualifying action
4. **Recovery Logic**: Uses EvalPlanQual (EPQ) to handle concurrent modifications and re-evaluate conditions

The function can restart processing from the beginning when concurrent updates are detected, potentially switching from MATCHED to NOT MATCHED BY SOURCE actions. It ensures forward progress by following update chains and never switches back to MATCHED actions once processing NOT MATCHED BY SOURCE actions.

## Parameters / Member Variables
- : ModifyTableContext containing execution state and context information
- : ResultRelInfo structure with information about the target relation
- : ItemPointer to the target tuple for table-based operations (NULL for view operations)
- : HeapTuple representing the target tuple for view-based operations (NULL for table operations)
- : Boolean indicating whether command tags can be set during execution
- : Pointer to boolean that tracks match status; may be modified to false if concurrent updates cause tuples to no longer match

## Dependencies
- Functions called/Symbols referenced:
  - [ExecQual](ExecQual.md)
  - [ExecProject](ExecProject.md)
  - [ExecUpdatePrologue](ExecUpdatePrologue.md)
  - [ExecUpdateAct](ExecUpdateAct.md)
  - [ExecUpdateEpilogue](ExecUpdateEpilogue.md)
  - [ExecDeletePrologue](ExecDeletePrologue.md)
  - [ExecDeleteAct](ExecDeleteAct.md)
  - [ExecDeleteEpilogue](ExecDeleteEpilogue.md)
  - [ExecProcessReturning](ExecProcessReturning.md)
  - [EvalPlanQual](EvalPlanQual.md)
  - [EvalPlanQualSlot](EvalPlanQualSlot.md)
  - [table_tuple_lock](../t/table_tuple_lock.md)
  - [table_tuple_fetch_row_version](../t/table_tuple_fetch_row_version.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
- Called from (representative examples):
  - [ExecMerge](ExecMerge.md)

## Notes and Other Information
- Implements sophisticated concurrent update handling using EvalPlanQual mechanism
- Supports Row Level Security (RLS) policy checks for UPDATE and DELETE operations
- Handles INSTEAD OF triggers for view operations
- Can process both regular table operations and view operations based on input parameters
- Uses tuple locking mechanisms to ensure consistency during concurrent access
- Maintains statistics counters for merged updated and deleted tuples
- Implements proper error handling for serialization failures and cardinality violations
- The function may loop back to reprocess actions when concurrent updates are detected (goto lmerge_matched)
- Supports cross-partition updates by detecting and handling partition movement scenarios

## Simplified Source

```c
static TupleTableSlot *
ExecMergeMatched(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
                 ItemPointer tupleid, HeapTuple oldtuple, bool canSetTag,
                 bool *matched)
{
    ModifyTableState *mtstate = context->mtstate;
    List **mergeActions = resultRelInfo->ri_MergeActions;
    ExprContext *econtext = mtstate->ps.ps_ExprContext;
    List *actionStates;
    TupleTableSlot *rslot = NULL;

    // Early exit if no actions to process
    if (mergeActions[MERGE_WHEN_MATCHED] == NIL &&
        mergeActions[MERGE_WHEN_NOT_MATCHED_BY_SOURCE] == NIL)
        return NULL;

    // Set up expression context with target tuple and source tuple
    econtext->ecxt_scantuple = resultRelInfo->ri_oldTupleSlot;
    econtext->ecxt_innertuple = context->planSlot;
    econtext->ecxt_outertuple = NULL;

    // Fetch the target tuple into oldTupleSlot
    if (oldtuple != NULL) {
        ExecForceStoreHeapTuple(oldtuple, resultRelInfo->ri_oldTupleSlot, false);
    } else {
        if (!table_tuple_fetch_row_version(resultRelInfo->ri_RelationDesc,
                                          tupleid, SnapshotAny,
                                          resultRelInfo->ri_oldTupleSlot))
            elog(ERROR, "failed to fetch the target tuple");
    }

retry_actions:
    // Determine action list based on join condition
    if (ExecQual(resultRelInfo->ri_MergeJoinCondition, econtext))
        actionStates = mergeActions[MERGE_WHEN_MATCHED];
    else
        actionStates = mergeActions[MERGE_WHEN_NOT_MATCHED_BY_SOURCE];

    // Find and execute the first qualifying action
    foreach(ListCell *l, actionStates) {
        MergeActionState *relaction = (MergeActionState *) lfirst(l);
        CmdType commandType = relaction->mas_action->commandType;
        TM_Result result;

        // Check action's WHEN condition
        if (!ExecQual(relaction->mas_whenqual, econtext))
            continue;

        // Apply RLS checks if needed
        if (resultRelInfo->ri_WithCheckOptions && commandType != CMD_NOTHING) {
            ExecWithCheckOptions(commandType == CMD_UPDATE ?
                               WCO_RLS_MERGE_UPDATE_CHECK : WCO_RLS_MERGE_DELETE_CHECK,
                               resultRelInfo, resultRelInfo->ri_oldTupleSlot,
                               context->mtstate->ps.state);
        }

        // Execute the action
        switch (commandType) {
            case CMD_UPDATE:
                {
                    TupleTableSlot *newslot = ExecProject(relaction->mas_proj);

                    if (!ExecUpdatePrologue(context, resultRelInfo, tupleid, NULL, newslot, &result))
                        break; // Concurrent modification

                    result = ExecUpdateAct(context, resultRelInfo, tupleid,
                                         NULL, newslot, canSetTag, &updateCxt);

                    if (result == TM_Ok) {
                        ExecUpdateEpilogue(context, &updateCxt, resultRelInfo,
                                         tupleid, NULL, newslot);
                        mtstate->mt_merge_updated += 1;
                    }
                }
                break;

            case CMD_DELETE:
                if (!ExecDeletePrologue(context, resultRelInfo, tupleid, NULL, NULL, &result))
                    break; // Concurrent modification

                result = ExecDeleteAct(context, resultRelInfo, tupleid, false);

                if (result == TM_Ok) {
                    ExecDeleteEpilogue(context, resultRelInfo, tupleid, NULL, false);
                    mtstate->mt_merge_deleted += 1;
                }
                break;

            case CMD_NOTHING:
                result = TM_Ok; // Do nothing is always successful
                break;
        }

        // Handle concurrent modifications
        if (result == TM_Updated || result == TM_Deleted) {
            // Re-lock tuple and retry if still processing MATCHED actions
            if (relaction->mas_action->matchKind == MERGE_WHEN_MATCHED) {
                // Use EvalPlanQual to get updated tuple and recheck conditions
                // Simplified: just retry with new tuple state
                goto retry_actions;
            }
            *matched = false; // Switch to NOT MATCHED processing
            return NULL;
        }

        // Process RETURNING clause if present
        if (resultRelInfo->ri_projectReturning) {
            switch (commandType) {
                case CMD_UPDATE:
                    rslot = ExecProcessReturning(resultRelInfo, newslot, context->planSlot);
                    break;
                case CMD_DELETE:
                    rslot = ExecProcessReturning(resultRelInfo,
                                               resultRelInfo->ri_oldTupleSlot,
                                               context->planSlot);
                    break;
            }
        }

        // Update processed count and exit (only one action executes)
        if (canSetTag && commandType != CMD_NOTHING)
            (context->estate->es_processed)++;
        break;
    }

    return rslot;
}
```