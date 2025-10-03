# advance_windowaggregate

## Location
[src/backend/executor/nodeWindowAgg.c:242-418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L242-L418)

## Overview
Advances a window aggregate function by processing one input tuple, updating the aggregate's transition value according to the aggregate's transition function.

## Definition

```c
static void
advance_windowaggregate(WindowAggState *winstate,
						WindowStatePerFunc perfuncstate,
						WindowStatePerAgg peraggstate)
```
## Detailed Description
This function is parallel to  in nodeAgg.c and handles the core logic for advancing window aggregate computations. It evaluates the aggregate's arguments from the current tuple, handles filtering conditions, manages strict transition functions, and carefully manages memory contexts and data copying. The function includes special handling for moving aggregates by tracking transition value counts and ensuring moving-aggregate transition functions don't return NULL. It also optimizes memory management by detecting when transition functions return pointers to their inputs or expanded objects already in the correct context.

## Parameters / Member Variables
- `*winstate`: The overall window aggregate execution state containing temporary contexts and current aggregate context
- `perfuncstate`: Per-function state containing the window function expression state, number of arguments, and collation information
- `peraggstate`: Per-aggregate state containing transition function info, current transition values, and memory context
## Dependencies
- Functions called/Symbols referenced:
  - [ExecEvalExpr](../E/ExecEvalExpr.md)
  - [datumCopy](../d/datumCopy.md)
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - DatumIsReadWriteExpandedObject
  - [DatumGetEOHP](../D/DatumGetEOHP.md)
  - [MemoryContextGetParent](../M/MemoryContextGetParent.md)
  - [DeleteExpandedObject](../D/DeleteExpandedObject.md)
- Called from (representative examples):
  - [eval_windowaggregates](../e/eval_windowaggregates.md)

## Notes and Other Information
- Handles FILTER clauses by evaluating the filter expression and skipping filtered-out tuples
- For strict transition functions, skips processing when any argument is NULL
- Special logic for strict functions with NULL initial values: uses first non-NULL input as initial state  
- Tracks  to support inverse transition functions in moving window aggregates
- Moving-aggregate transition functions must not return NULL (enforced with error)
- Sophisticated memory management including detection of expanded objects already in the correct memory context
- Sets curaggcontext during transition function calls to support AggCheckCallContext

## Simplified Source

```c
static void
advance_windowaggregate(WindowAggState *winstate,
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
            return;
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

    // Handle strict transition functions
    if (peraggstate->transfn.fn_strict)
    {
        // Skip if any argument is NULL
        for (i = 1; i <= numArguments; i++)
        {
            if (fcinfo->args[i].isnull)
            {
                MemoryContextSwitchTo(oldContext);
                return;
            }
        }

        // For strict functions with NULL initial value, use first non-NULL input
        if (peraggstate->transValueCount == 0 && peraggstate->transValueIsNull)
        {
            MemoryContextSwitchTo(peraggstate->aggcontext);
            peraggstate->transValue = datumCopy(fcinfo->args[1].value,
                                               peraggstate->transtypeByVal,
                                               peraggstate->transtypeLen);
            peraggstate->transValueIsNull = false;
            peraggstate->transValueCount = 1;
            MemoryContextSwitchTo(oldContext);
            return;
        }

        // Don't call strict function with NULL transition value
        if (peraggstate->transValueIsNull)
        {
            MemoryContextSwitchTo(oldContext);
            Assert(!OidIsValid(peraggstate->invtransfn_oid));
            return;
        }
    }

    // Call the transition function
    InitFunctionCallInfoData(*fcinfo, &(peraggstate->transfn),
                            numArguments + 1,
                            perfuncstate->winCollation,
                            (void *) winstate, NULL);
    fcinfo->args[0].value = peraggstate->transValue;
    fcinfo->args[0].isnull = peraggstate->transValueIsNull;
    winstate->curaggcontext = peraggstate->aggcontext;
    newVal = FunctionCallInvoke(fcinfo);
    winstate->curaggcontext = NULL;

    // Moving-aggregate transition functions must not return NULL
    if (fcinfo->isnull && OidIsValid(peraggstate->invtransfn_oid))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("moving-aggregate transition function must not return null")));

    // Track number of rows for inverse transition support
    peraggstate->transValueCount++;

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

        // Free old transition value if not NULL
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
}
```