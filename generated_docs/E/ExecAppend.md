# ExecAppend

## Location
[src/backend/executor/nodeAppend.c:288-385](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAppend.c#L288-L385)

## Overview
The main execution function for Append nodes that handles iteration over multiple subplans, supporting both synchronous and asynchronous execution modes.

## Definition

```c
static TupleTableSlot *
ExecAppend(PlanState *pstate)
```
## Detailed Description
ExecAppend is the core execution function for PostgreSQL's Append node executor. It implements a sophisticated iteration strategy that can handle both synchronous and asynchronous subplan execution. The function operates in a continuous loop, cycling through available subplans until all are exhausted.

Key execution phases:
1. **Initialization Check**: On first call after Init or ReScan, performs setup including async subplan initialization
2. **Subplan Selection**: Uses a pluggable strategy (choose_next_subplan function pointer) to select the next subplan to execute
3. **Tuple Retrieval**: Attempts to get tuples from both async and sync subplans
4. **Event Handling**: Manages async events and waits for completion when necessary
5. **Termination**: Returns NULL when all subplans are exhausted

The function optimizes performance by returning tuples directly from subplans without copying through the Append node's result slot.

## Parameters / Member Variables
- `*pstate`: The PlanState (cast to AppendState) containing the append execution context
## Dependencies
- Functions called/Symbols referenced:
  - castNode (for type casting to AppendState)
  - [ExecClearTuple](ExecClearTuple.md) (for returning empty results)
  - [ExecAppendAsyncBegin](ExecAppendAsyncBegin.md) (for async subplan initialization)
  - [ExecAppendAsyncGetNext](ExecAppendAsyncGetNext.md) (for async tuple retrieval)
  - [ExecAppendAsyncEventWait](ExecAppendAsyncEventWait.md) (for async event processing)
  - [ExecProcNode](ExecProcNode.md) (for synchronous subplan execution)
  - TupIsNull (for null tuple checking)
  - bms_is_empty (for bitmap set operations)
  - CHECK_FOR_INTERRUPTS (for query cancellation support)
- Called from (representative examples):
  - [ExecInitAppend](ExecInitAppend.md) (set as ExecProcNode function pointer)
  - PostgreSQL executor framework through function pointer calls

## Notes and Other Information
- The function supports both synchronous and asynchronous execution modes simultaneously
- Tuples are returned directly from subplans without intermediate copying for performance
- The function handles partition pruning by working with dynamically determined valid subplans  
- Async execution allows for parallel processing of subplans that support it
- The execution strategy is pluggable via the choose_next_subplan function pointer
- [Query](../Q/Query.md) interruption is supported through CHECK_FOR_INTERRUPTS() macro calls

## Simplified Source

```c
static TupleTableSlot *ExecAppend(PlanState *pstate) {
    AppendState *node = castNode(AppendState, pstate);
    TupleTableSlot *result;

    // Initialize on first call
    if (!node->as_begun) {
        if (node->as_nplans == 0)
            return ExecClearTuple(node->ps.ps_ResultTupleSlot);

        // Start async subplans if any
        if (node->as_nasyncplans > 0)
            ExecAppendAsyncBegin(node);

        // Choose first sync subplan
        if (!node->choose_next_subplan(node) && node->as_nasyncremain == 0)
            return ExecClearTuple(node->ps.ps_ResultTupleSlot);

        node->as_begun = true;
    }

    // Main execution loop
    for (;;) {
        CHECK_FOR_INTERRUPTS();

        // Try async subplans first if sync is done or async needs attention
        if (node->as_syncdone || !bms_is_empty(node->as_needrequest)) {
            if (ExecAppendAsyncGetNext(node, &result))
                return result;
        }

        // Execute current sync subplan
        PlanState *subnode = node->appendplans[node->as_whichplan];
        result = ExecProcNode(subnode);

        if (!TupIsNull(result))
            return result;

        // Wait for async events before choosing next subplan
        if (node->as_nasyncremain > 0)
            ExecAppendAsyncEventWait(node);

        // Choose next subplan; exit if none available
        if (!node->choose_next_subplan(node) && node->as_nasyncremain == 0)
            return ExecClearTuple(node->ps.ps_ResultTupleSlot);
    }
}
```