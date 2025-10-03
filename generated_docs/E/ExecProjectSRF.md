# ExecProjectSRF

## Location
[src/backend/executor/nodeProjectSet.c:139-226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeProjectSet.c#L139-L226)

## Overview
ExecProjectSRF projects a targetlist containing one or more set-returning functions, handling the complex evaluation state of SRFs.

## Definition
```c
static TupleTableSlot *ExecProjectSRF(ProjectSetState *node, bool continuing)
```

## Detailed Description
ExecProjectSRF is the core function responsible for evaluating target list expressions that contain set-returning functions. It handles the intricate state management required for SRFs, including:

1. **State Management**: Tracks which SRFs have finished producing results (`ExprEndResult`) vs. those still producing (`ExprMultipleResult`)
2. **Continuation Logic**: When `continuing` is true, it handles previously started SRF evaluation
3. **Mixed Expression Types**: Handles both SRFs (SetExprState) and regular expressions in the same target list
4. **Memory Context Management**: Evaluates expressions in the appropriate per-tuple memory context

The function iterates through all elements in the target list, evaluating each according to its type and current state, and assembles the results into a virtual tuple slot.

## Parameters / Member Variables
- `node`: The ProjectSetState containing evaluation state and target list information
- `continuing`: Boolean indicating whether to continue projecting from the same input tuple (true) or start fresh with a new input tuple (false)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecClearTuple](ExecClearTuple.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [ExecMakeFunctionResultSet](ExecMakeFunctionResultSet.md)
  - [ExecEvalExpr](ExecEvalExpr.md)
  - [ExecStoreVirtualTuple](ExecStoreVirtualTuple.md)
- Called from (representative examples):
  - [ExecProjectSet](ExecProjectSet.md) (both for continuing and new evaluations)

## Notes and Other Information
- Uses assertions to ensure ProjectSet nodes only contain SRFs (hassrf must be true)
- Returns NULL when all SRFs have finished producing results
- Carefully manages memory contexts to prevent leaks during SRF evaluation
- The elemdone array tracks the completion state of each target list element

## Simplified Source

```c
static TupleTableSlot *
ExecProjectSRF(ProjectSetState *node, bool continuing)
{
    TupleTableSlot *resultSlot = node->ps.ps_ResultTupleSlot;
    ExprContext *econtext = node->ps.ps_ExprContext;
    MemoryContext oldcontext;
    bool hasresult = false;
    int argno;

    ExecClearTuple(resultSlot);

    // Evaluate expressions in per-tuple memory context
    oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    // Assume no more tuples unless SRF produces more
    node->pending_srf_tuples = false;

    // Process each element in the target list
    for (argno = 0; argno < node->nelems; argno++)
    {
        Node *elem = node->elems[argno];
        ExprDoneCond *isdone = &node->elemdone[argno];
        Datum *result = &resultSlot->tts_values[argno];
        bool *isnull = &resultSlot->tts_isnull[argno];

        if (continuing && *isdone == ExprEndResult)
        {
            // SRF exhausted, return NULL for this column
            *result = (Datum) 0;
            *isnull = true;
        }
        else if (IsA(elem, SetExprState))
        {
            // Evaluate set-returning function
            *result = ExecMakeFunctionResultSet((SetExprState *) elem,
                                                econtext, node->argcontext,
                                                isnull, isdone);

            if (*isdone != ExprEndResult)
                hasresult = true;
            if (*isdone == ExprMultipleResult)
                node->pending_srf_tuples = true;
        }
        else
        {
            // Regular expression, evaluate normally
            *result = ExecEvalExpr((ExprState *) elem, econtext, isnull);
            *isdone = ExprSingleResult;
        }
    }

    MemoryContextSwitchTo(oldcontext);

    // Return result tuple if any SRF produced output
    if (hasresult)
    {
        ExecStoreVirtualTuple(resultSlot);
        return resultSlot;
    }

    return NULL;
}
```