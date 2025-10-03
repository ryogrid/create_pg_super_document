# ExecDeleteEpilogue

## Location
[src/backend/executor/nodeModifyTable.c:1391-1448](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L1391-L1448)

## Overview
A subroutine for ExecDelete that handles the closing steps of tuple deletion, including firing AFTER FOR EACH ROW triggers and managing transition tables for cross-partition tuple moves.

## Definition

```c
static void
ExecDeleteEpilogue(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
				   ItemPointer tupleid, HeapTuple oldtuple, bool changingPart)
```
## Detailed Description
ExecDeleteEpilogue performs the final steps after a tuple has been successfully deleted from the table. Its primary responsibilities include:

1. **Transition Table Management**: When a DELETE is part of a cross-partition UPDATE (tuple movement), it ensures the old tuple is properly captured in the transition OLD TABLE for statement-level triggers.

2. **Trigger Execution**: Fires AFTER ROW DELETE triggers with the appropriate transition capture state.

3. **Cross-Partition Coordination**: Handles the special case where a DELETE is actually part of a cross-partition UPDATE operation, ensuring proper trigger semantics are maintained.

The function carefully manages transition capture to avoid double-capturing tuples when both UPDATE and DELETE triggers need to fire during cross-partition operations.

## Parameters / Member Variables
- `*context`: ModifyTableContext containing the execution state and metadata for the modify operation
- `*resultRelInfo`: Information about the result relation from which the tuple is being deleted
- `tupleid`: ItemPointer identifying the physical location of the deleted tuple
- `oldtuple`: HeapTuple containing the actual tuple data that was deleted
- `changingPart`: Boolean indicating whether this deletion is part of a cross-partition tuple move
## Dependencies
- Functions called/Symbols referenced:
  - [ExecARUpdateTriggers](ExecARUpdateTriggers.md) (for cross-partition UPDATE transition handling)
  - [ExecARDeleteTriggers](ExecARDeleteTriggers.md) (for AFTER ROW DELETE triggers)
  - CMD_UPDATE (command type constant)
- Called from:
  - [ExecDelete](ExecDelete.md) (main deletion execution function)
  - [ExecMergeMatched](ExecMergeMatched.md) (MERGE statement execution)

## Notes and Other Information
- This is a static function, only accessible within nodeModifyTable.c
- The function handles a complex interaction between DELETE and UPDATE operations during cross-partition moves
- Transition capture state is carefully managed to prevent duplicate entries in transition tables
- The changingPart parameter affects how AFTER DELETE triggers are executed
- Part of PostgreSQL's execution engine for DML operations

## Simplified Source

```c
static void ExecDeleteEpilogue(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
                              ItemPointer tupleid, HeapTuple oldtuple, bool changingPart)
{
    ModifyTableState *mtstate = context->mtstate;
    EState *estate = context->estate;
    TransitionCaptureState *ar_delete_trig_tcs = mtstate->mt_transition_capture;

    // Handle cross-partition DELETE as part of UPDATE operation
    if (mtstate->operation == CMD_UPDATE && mtstate->mt_transition_capture &&
        mtstate->mt_transition_capture->tcs_update_old_table)
    {
        // Capture tuple for UPDATE OLD TABLE transition
        ExecARUpdateTriggers(estate, resultRelInfo,
                           NULL, NULL,
                           tupleid, oldtuple,
                           NULL, NULL, mtstate->mt_transition_capture,
                           false);

        // Prevent double-capturing in DELETE triggers
        ar_delete_trig_tcs = NULL;
    }

    // Fire AFTER ROW DELETE triggers
    ExecARDeleteTriggers(estate, resultRelInfo, tupleid, oldtuple,
                        ar_delete_trig_tcs, changingPart);
}
```