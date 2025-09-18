# AggCheckCallContext

## Location
src/backend/executor/nodeAgg.c: 4511 - 4554

## Overview
AggCheckCallContext tests whether a SQL function is being called in an aggregate context and optionally returns the memory context for transition values.

## Definition
```c
int AggCheckCallContext(FunctionCallInfo fcinfo, MemoryContext *aggcontext)
```

## Detailed Description
This function provides a way for aggregate transition and final functions to verify they are being called within an aggregate execution context rather than as plain SQL functions. It examines the function call information to determine the calling context and returns specific codes indicating the type of aggregate context (AGG_CONTEXT_AGGREGATE for regular aggregates, AGG_CONTEXT_WINDOW for window functions). When the optional aggcontext parameter is provided, the function also returns the memory context where aggregate transition values should be stored, which is crucial for proper memory management in aggregate functions.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing context about how the function was called
- `aggcontext`: Optional output parameter that receives the memory context for storing aggregate transition values

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - AGG_CONTEXT_AGGREGATE
  - AGG_CONTEXT_WINDOW
- Called from (representative examples):
  - array_agg_transfn (src/backend/utils/adt/array_userfuncs.c:497)
  - numeric_combine (src/backend/utils/adt/numeric.c:5063)
  - string_agg_finalfn (src/backend/utils/adt/varlena.c:5363)
  - json_agg_transfn_worker (src/backend/utils/adt/json.c:777)
  - makeNumericAggState (src/backend/utils/adt/numeric.c:4839)

## Notes and Other Information
- Part of the public API exposed to aggregate functions for context validation
- Returns 0 if not called in an aggregate context, non-zero values for different aggregate types
- Critical for aggregate functions that need different behavior when called as aggregates vs. regular functions
- The aggcontext parameter enables proper memory management by providing the correct memory context for transition values
- Used extensively throughout PostgreSQL's built-in aggregate functions for validation and memory management
- The memory context returned should not be cached in fn_extra due to potential interleaving of calls with different contexts