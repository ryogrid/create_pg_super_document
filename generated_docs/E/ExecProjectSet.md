# ExecProjectSet

## Location
[src/backend/executor/nodeProjectSet.c:42-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeProjectSet.c#L42-L138)

## Overview
ExecProjectSet is the main execution function for ProjectSet plan nodes that handles evaluation of target lists containing set-returning functions (SRFs).

## Definition

```c
static TupleTableSlot *
ExecProjectSet(PlanState *pstate)
```
## Detailed Description
ExecProjectSet manages the execution of ProjectSet nodes, which are responsible for projecting tuples that contain set-returning functions. The function operates in two main modes:

1. **Continuation mode**: When there are still pending tuples from a previous SRF evaluation ( is true), it attempts to project another tuple from the same input.

2. **New input mode**: When no pending tuples exist, it retrieves new input tuples from the outer plan and projects SRFs from them.

The function handles the complex lifecycle of SRFs by maintaining state about whether more tuples are expected from the current input tuple. It continues processing until either a valid result tuple is produced or no more input tuples are available.

## Parameters / Member Variables
- `*pstate`: The plan state node, which is cast to ProjectSetState internally
## Dependencies
- Functions called/Symbols referenced:
  - [ExecProjectSRF](ExecProjectSRF.md)
  - ResetExprContext  
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [ExecProcNode](ExecProcNode.md)
  - TupIsNull
  - outerPlanState
- Called from (representative examples):
  - [ExecInitProjectSet](ExecInitProjectSet.md) (assigned as the ExecProcNode function)

## Notes and Other Information
- The function includes interrupt checking via CHECK_FOR_INTERRUPTS()
- Memory management is carefully handled with separate contexts for per-tuple and argument evaluation
- The function loops until it finds an input tuple that produces at least one output row
- Designed to handle the complex semantics of set-returning functions in PostgreSQL's execution engine

## Simplified Source

```c
static TupleTableSlot *ExecProjectSet(PlanState *pstate)
{
    ProjectSetState *node = castNode(ProjectSetState, pstate);
    ExprContext *econtext = node->ps.ps_ExprContext;

    CHECK_FOR_INTERRUPTS();

    // Reset per-tuple expression context
    ResetExprContext(econtext);

    // Check if still processing tuples from previous SRF evaluation
    if (node->pending_srf_tuples)
    {
        TupleTableSlot *resultSlot = ExecProjectSRF(node, true);
        if (resultSlot != NULL)
            return resultSlot;
    }

    // Main loop to get new input tuples and project SRFs
    for (;;)
    {
        // Reset argument context for memory management
        MemoryContextReset(node->argcontext);

        // Get next tuple from outer plan
        PlanState *outerPlan = outerPlanState(node);
        TupleTableSlot *outerTupleSlot = ExecProcNode(outerPlan);

        // No more input tuples
        if (TupIsNull(outerTupleSlot))
            return NULL;

        // Setup input tuple for projection
        econtext->ecxt_outertuple = outerTupleSlot;

        // Evaluate SRF expressions and project result
        TupleTableSlot *resultSlot = ExecProjectSRF(node, false);

        // Return result if projection produced rows
        if (resultSlot)
            return resultSlot;

        // Reset context before looping for next input tuple
        ResetExprContext(econtext);
    }

    return NULL;
}
```