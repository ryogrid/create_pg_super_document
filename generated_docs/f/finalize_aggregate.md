# finalize_aggregate

## Location
[src/backend/executor/nodeAgg.c:1046-1145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L1046-L1145)

## Overview
Computes the final value of a single aggregate by applying the finalfn (if present) or returning the transition value, handling direct arguments and ensuring proper memory management.

## Definition
```c
static void finalize_aggregate(AggState *aggstate,
                             AggStatePerAgg peragg,
                             AggStatePerGroup pergroupstate,
                             Datum *resultVal, bool *resultIsNull)
```

## Detailed Description
This function computes the final result of an aggregate function for one grouping set. It represents the culmination of the aggregate computation process, taking the accumulated transition state and producing the final output value that will be returned to the user.

The function handles two main scenarios:
1. **With finalfn**: Evaluates direct arguments, sets up function call context, and invokes the aggregate's final function with the transition state and direct arguments
2. **Without finalfn**: Returns the transition value directly as the final result

Key features and safeguards:
- Evaluates direct arguments even when no finalfn exists to ensure side effects occur as expected
- Uses MakeExpandedObjectReadOnly to prevent modification of shared aggregate results
- Handles strict functions by returning NULL when any input is NULL
- Manages memory contexts properly, executing finalfn in output-tuple context
- Preserves the integrity of transition state since it might be shared with other aggregates

## Parameters / Member Variables
- `aggstate`: Main aggregate state containing expression context and current aggregate tracking
- `peragg`: Per-aggregate state containing finalfn information, direct arguments, and type metadata
- `pergroupstate`: Per-group state containing the transition value to be finalized
- `resultVal`: Output parameter to receive the computed final result value
- `resultIsNull`: Output parameter to indicate whether the final result is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [ExecEvalExpr](../E/ExecEvalExpr.md)
  - InitFunctionCallInfoData
  - MakeExpandedObjectReadOnly
  - FunctionCallInvoke
- Macros used:
  - LOCAL_FCINFO
  - FUNC_MAX_ARGS
- Data types used:
  - [AggState](../A/AggState.md)
  - [AggStatePerAgg](../A/AggStatePerAgg.md)
  - [AggStatePerGroup](../A/AggStatePerGroup.md)
  - [AggStatePerTrans](../A/AggStatePerTrans.md)
- Called from (representative examples):
  - [finalize_aggregates](finalize_aggregates.md)

## Notes and Other Information
- Handles only one grouping set (already set in aggstate->current_set) per invocation
- The result is delivered in output-tuple context regardless of caller's current memory context
- Non-destructive operation - does not modify the transition state since it may be shared
- Uses MakeExpandedObjectReadOnly to ensure returned expanded datums are read-only
- Direct arguments are placed in argument positions 1 and up, with position 0 reserved for transition state
- Properly handles the case where finalfn has fewer arguments than direct args + transition state by filling remaining positions with NULLs
- Sets aggstate->curperagg during finalfn execution to support AggGetAggref() calls
- Critical for aggregate result correctness - any bugs here affect final query results

## Simplified Source

```c
static void
finalize_aggregate(AggState *aggstate,
                   AggStatePerAgg peragg,
                   AggStatePerGroup pergroupstate,
                   Datum *resultVal, bool *resultIsNull)
{
    LOCAL_FCINFO(fcinfo, FUNC_MAX_ARGS);
    bool anynull = false;
    MemoryContext oldContext;
    int i;
    ListCell *lc;
    AggStatePerTrans pertrans = &aggstate->pertrans[peragg->transno];

    // Switch to output context for result computation
    oldContext = MemoryContextSwitchTo(aggstate->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

    // Evaluate direct arguments (starting at position 1)
    i = 1;
    foreach(lc, peragg->aggdirectargs)
    {
        ExprState *expr = (ExprState *) lfirst(lc);

        fcinfo->args[i].value = ExecEvalExpr(expr,
                                           aggstate->ss.ps.ps_ExprContext,
                                           &fcinfo->args[i].isnull);
        anynull |= fcinfo->args[i].isnull;
        i++;
    }

    // Apply finalfn if present, otherwise return transition value
    if (OidIsValid(peragg->finalfn_oid))
    {
        int numFinalArgs = peragg->numFinalArgs;

        // Setup function call context
        aggstate->curperagg = peragg;
        InitFunctionCallInfoData(*fcinfo, &peragg->finalfn,
                               numFinalArgs, pertrans->aggCollation,
                               (void *) aggstate, NULL);

        // Position 0: transition state value
        fcinfo->args[0].value =
            MakeExpandedObjectReadOnly(pergroupstate->transValue,
                                     pergroupstate->transValueIsNull,
                                     pertrans->transtypeLen);
        fcinfo->args[0].isnull = pergroupstate->transValueIsNull;
        anynull |= pergroupstate->transValueIsNull;

        // Fill remaining positions with NULLs if needed
        for (; i < numFinalArgs; i++)
        {
            fcinfo->args[i].value = (Datum) 0;
            fcinfo->args[i].isnull = true;
            anynull = true;
        }

        // Call finalfn or return NULL for strict functions with NULL inputs
        if (fcinfo->flinfo->fn_strict && anynull)
        {
            *resultVal = (Datum) 0;
            *resultIsNull = true;
        }
        else
        {
            Datum result = FunctionCallInvoke(fcinfo);
            *resultIsNull = fcinfo->isnull;
            *resultVal = MakeExpandedObjectReadOnly(result, fcinfo->isnull,
                                                  peragg->resulttypeLen);
        }
        aggstate->curperagg = NULL;
    }
    else
    {
        // No finalfn - return transition value directly
        *resultVal =
            MakeExpandedObjectReadOnly(pergroupstate->transValue,
                                     pergroupstate->transValueIsNull,
                                     pertrans->transtypeLen);
        *resultIsNull = pergroupstate->transValueIsNull;
    }

    MemoryContextSwitchTo(oldContext);
}
```