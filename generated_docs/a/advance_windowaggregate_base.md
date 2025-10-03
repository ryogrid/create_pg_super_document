# advance_windowaggregate_base

## Location
[src/backend/executor/nodeWindowAgg.c:419-581](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L419-L581)

## Overview
Removes the oldest tuple from a window aggregate by calling the aggregate's inverse transition function, supporting efficient sliding window computations.

## Definition

```c
static bool
advance_windowaggregate_base(WindowAggState *winstate,
							 WindowStatePerFunc perfuncstate,
							 WindowStatePerAgg peraggstate)
```
## Detailed Description
This function is the counterpart to  and handles the removal of tuples from moving window aggregates. It calls the inverse transition function to efficiently remove the contribution of the oldest tuple from the aggregate's current state. The function includes several safety mechanisms: it validates that the aggregate state is not NULL (as required for moving aggregates), handles the special case of removing the last tuple by reinitializing the aggregate, and returns false if the inverse transition function indicates it cannot perform the removal (forcing a restart). Like its forward counterpart, it handles filtering, strict functions, and careful memory management.

## Parameters / Member Variables
- `*winstate`: The overall window aggregate execution state containing temporary contexts and current aggregate context
- `perfuncstate`: Per-function state containing the window function expression state, number of arguments, and collation information
- `peraggstate`: Per-aggregate state containing inverse transition function info, current transition values, and memory context
## Dependencies
- Functions called/Symbols referenced:
  - [ExecEvalExpr](../E/ExecEvalExpr.md)
  - [initialize_windowaggregate](../i/initialize_windowaggregate.md)
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - DatumIsReadWriteExpandedObject
  - [DatumGetEOHP](../D/DatumGetEOHP.md)
  - [MemoryContextGetParent](../M/MemoryContextGetParent.md)
  - [datumCopy](../d/datumCopy.md)
  - [DeleteExpandedObject](../D/DeleteExpandedObject.md)
- Called from (representative examples):
  - [eval_windowaggregates](../e/eval_windowaggregates.md)

## Notes and Other Information
- Returns  if successful,  if the inverse transition function cannot handle the removal (forcing aggregate restart)
- Handles FILTER clauses by evaluating the filter expression and returning success immediately for filtered-out tuples
- For strict inverse transition functions, skips processing when any argument is NULL and returns success
- When  reaches 1, reinitializes the aggregate instead of calling the inverse function to ensure proper initial state
- Enforces that the transition state must not be NULL in moving-aggregate mode (throws error if violated)
- Decrements  to track the number of tuples contributing to the current state
- Same sophisticated memory management as  including expanded object detection
- Sets curaggcontext during inverse transition function calls to support AggCheckCallContext

## Simplified Source

```c
static bool
advance_windowaggregate_base(WindowAggState *winstate,
                            WindowStatePerFunc perfuncstate,
                            WindowStatePerAgg peraggstate)
{
    LOCAL_FCINFO(fcinfo, FUNC_MAX_ARGS);
    WindowFuncExprState *wfuncstate = perfuncstate->wfuncstate;
    int numArguments = perfuncstate->numArguments;
    Datum newVal;
    ListCell *arg;
    int i;
    MemoryContext oldContext;
    ExprContext *econtext = winstate->tmpcontext;
    ExprState *filter = wfuncstate->aggfilter;

    oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    // Skip anything filtered out
    if (filter)
    {
        bool isnull;
        Datum res = ExecEvalExpr(filter, econtext, &isnull);

        if (isnull || !DatumGetBool(res))
        {
            MemoryContextSwitchTo(oldContext);
            return true;
        }
    }

    // Evaluate aggregate arguments (start from 1, 0 is transition value)
    i = 1;
    foreach(arg, wfuncstate->args)
    {
        ExprState *argstate = (ExprState *) lfirst(arg);
        fcinfo->args[i].value = ExecEvalExpr(argstate, econtext,
                                           &fcinfo->args[i].isnull);
        i++;
    }

    // Handle strict inverse transition functions
    if (peraggstate->invtransfn.fn_strict)
    {
        // Skip if any argument is NULL
        for (i = 1; i <= numArguments; i++)
        {
            if (fcinfo->args[i].isnull)
            {
                MemoryContextSwitchTo(oldContext);
                return true;
            }
        }
    }

    // Validate that we have rows to remove
    Assert(peraggstate->transValueCount > 0);

    // Moving aggregates must not have NULL state
    if (peraggstate->transValueIsNull)
        elog(ERROR, "aggregate transition value is NULL before inverse transition");

    // Special case: removing the last tuple - reinitialize instead
    if (peraggstate->transValueCount == 1)
    {
        MemoryContextSwitchTo(oldContext);
        initialize_windowaggregate(winstate,
                                  &winstate->perfunc[peraggstate->wfuncno],
                                  peraggstate);
        return true;
    }

    // Call the inverse transition function
    InitFunctionCallInfoData(*fcinfo, &(peraggstate->invtransfn),
                            numArguments + 1,
                            perfuncstate->winCollation,
                            (void *) winstate, NULL);
    fcinfo->args[0].value = peraggstate->transValue;
    fcinfo->args[0].isnull = peraggstate->transValueIsNull;
    winstate->curaggcontext = peraggstate->aggcontext;
    newVal = FunctionCallInvoke(fcinfo);
    winstate->curaggcontext = NULL;

    // If inverse function returns NULL, we can't continue with this approach
    if (fcinfo->isnull)
    {
        MemoryContextSwitchTo(oldContext);
        return false;
    }

    // Update row count
    peraggstate->transValueCount--;

    // Handle memory management for pass-by-ref values
    if (!peraggstate->transtypeByVal &&
        DatumGetPointer(newVal) != DatumGetPointer(peraggstate->transValue))
    {
        if (!fcinfo->isnull)
        {
            MemoryContextSwitchTo(peraggstate->aggcontext);
            // Check if we can adopt expanded object without copying
            if (DatumIsReadWriteExpandedObject(newVal, false, peraggstate->transtypeLen) &&
                MemoryContextGetParent(DatumGetEOHP(newVal)->eoh_context) == CurrentMemoryContext)
                /* do nothing */;
            else
                newVal = datumCopy(newVal, peraggstate->transtypeByVal, peraggstate->transtypeLen);
        }

        // Free old transition value
        if (!peraggstate->transValueIsNull)
        {
            if (DatumIsReadWriteExpandedObject(peraggstate->transValue, false, peraggstate->transtypeLen))
                DeleteExpandedObject(peraggstate->transValue);
            else
                pfree(DatumGetPointer(peraggstate->transValue));
        }
    }

    MemoryContextSwitchTo(oldContext);
    peraggstate->transValue = newVal;
    peraggstate->transValueIsNull = fcinfo->isnull;

    return true;
}
```