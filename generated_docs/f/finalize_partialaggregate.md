# finalize_partialaggregate

## Location
[src/backend/executor/nodeAgg.c:1146-1203](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L1146-L1203)

## Overview
Computes the output value of one partial aggregate by optionally serializing the transition value before returning it in the output-tuple context.

## Definition

```c
static void
finalize_partialaggregate(AggState *aggstate,
						  AggStatePerAgg peragg,
						  AggStatePerGroup pergroupstate,
						  Datum *resultVal, bool *resultIsNull)
```
## Detailed Description
This function finalizes a partial aggregate by preparing its transition value for output. The key distinction from full aggregate finalization is that this function may apply a serialization function instead of a final function. If a serialization function is configured (serialfn_oid is valid), it serializes the transition value to create a portable representation. If no serialization function is needed, it simply returns the transition value as-is. The function operates in the output-tuple memory context to ensure proper memory management.

## Parameters / Member Variables
- : The overall aggregate execution state containing global information
- : Per-aggregate information including result type and configuration
- : Per-group state containing the current transition value and null flag
- : Output parameter to store the finalized aggregate value
- : Output parameter to indicate if the result is NULL

## Dependencies
- Functions called/Symbols referenced:
  - MakeExpandedObjectReadOnly
  - FunctionCallInvoke
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - OidIsValid
- Called from (representative examples):
  - [finalize_aggregates](finalize_aggregates.md)

## Notes and Other Information
- The serialization function will be run in the output-tuple context, making the caller's CurrentMemoryContext irrelevant
- Handles strict serialization functions properly by avoiding calls with NULL input when fn_strict is true
- Uses MakeExpandedObjectReadOnly to ensure proper handling of expanded objects in the result
- Part of PostgreSQL's partial aggregation mechanism used in parallel query processing and other advanced aggregate scenarios

## Simplified Source

```c
static void
finalize_partialaggregate(AggState *aggstate,
                         AggStatePerAgg peragg,
                         AggStatePerGroup pergroupstate,
                         Datum *resultVal, bool *resultIsNull)
{
    AggStatePerTrans pertrans = &aggstate->pertrans[peragg->transno];
    MemoryContext oldContext;

    // Switch to output context
    oldContext = MemoryContextSwitchTo(aggstate->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

    // Apply serialization function if configured
    if (OidIsValid(pertrans->serialfn_oid))
    {
        // Handle strict serialization functions with NULL input
        if (pertrans->serialfn.fn_strict && pergroupstate->transValueIsNull)
        {
            *resultVal = (Datum) 0;
            *resultIsNull = true;
        }
        else
        {
            FunctionCallInfo fcinfo = pertrans->serialfn_fcinfo;
            Datum result;

            // Setup function call with transition value
            fcinfo->args[0].value =
                MakeExpandedObjectReadOnly(pergroupstate->transValue,
                                         pergroupstate->transValueIsNull,
                                         pertrans->transtypeLen);
            fcinfo->args[0].isnull = pergroupstate->transValueIsNull;
            fcinfo->isnull = false;

            // Call serialization function
            result = FunctionCallInvoke(fcinfo);
            *resultIsNull = fcinfo->isnull;
            *resultVal = MakeExpandedObjectReadOnly(result, fcinfo->isnull,
                                                  peragg->resulttypeLen);
        }
    }
    else
    {
        // No serialization - return transition value directly
        *resultVal =
            MakeExpandedObjectReadOnly(pergroupstate->transValue,
                                     pergroupstate->transValueIsNull,
                                     pertrans->transtypeLen);
        *resultIsNull = pergroupstate->transValueIsNull;
    }

    MemoryContextSwitchTo(oldContext);
}
```