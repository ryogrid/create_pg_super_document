# AggGetTempMemoryContext

## Location
[src/backend/executor/nodeAgg.c:4589-4614](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L4589-L4614)

## Overview
Fetches a short-term memory context that aggregate final functions can safely reset as needed during aggregation processing.

## Definition
```c
MemoryContext AggGetTempMemoryContext(FunctionCallInfo fcinfo)
```

## Detailed Description
AggGetTempMemoryContext is a utility function designed specifically for aggregate final functions to obtain a temporary memory context that can be safely reset. The function checks if the provided FunctionCallInfo context is an AggState node, and if so, returns the per-tuple memory context from the aggregation's temporary context. This allows final functions to perform memory-intensive operations without worrying about memory leaks, as they can reset the context when done.

The function is explicitly not useful for transition functions since the returned context may be the same as the context in which transition functions are called. It's also not currently useful for aggregates called as window functions.

## Parameters / Member Variables
- `fcinfo`: FunctionCallInfo structure containing the function call context, expected to contain an AggState when called from aggregate functions

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - [AggState](AggState.md) (aggregate execution state structure)
  - [FunctionCallInfo](../F/FunctionCallInfo.md) (function call information structure)
- Called from (representative examples):
  - AGG_CONTEXT_WINDOW (referenced in include/fmgr.h)

## Notes and Other Information
- The function returns NULL if the context is not an AggState or if fcinfo->context is NULL
- This is specifically designed for final functions in aggregate processing
- The returned context comes from aggstate->tmpcontext->ecxt_per_tuple_memory
- Should not be used by transition functions as the behavior is not guaranteed to be safe
- Not suitable for window function aggregates in the current implementation

## Simplified Source

```c
MemoryContext AggGetTempMemoryContext(FunctionCallInfo fcinfo)
{
    if (fcinfo->context && IsA(fcinfo->context, AggState))
    {
        AggState *aggstate = (AggState *) fcinfo->context;

        // Return the temporary per-tuple memory context
        return aggstate->tmpcontext->ecxt_per_tuple_memory;
    }

    return NULL; // Not in aggregate context
}
```