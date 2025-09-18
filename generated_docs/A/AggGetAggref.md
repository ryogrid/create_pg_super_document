# AggGetAggref

## Location
src/backend/executor/nodeAgg.c: 4555 - 4588

## Overview
AggGetAggref allows aggregate support functions to retrieve the Aggref node for the aggregate call they are executing.

## Definition
```c
Aggref *AggGetAggref(FunctionCallInfo fcinfo)
```

## Detailed Description
This function provides a way for aggregate support functions (both transition and final functions) to access the Aggref node that represents the aggregate call being executed. It examines the function call context to determine if it's being called from within an aggregate and returns the appropriate Aggref. For merged aggregates sharing the same inputs and transition functions, transition functions may receive any of the applicable Aggrefs, so they should not rely on final-function-specific fields. Final functions, however, receive precise Aggref information. The function returns NULL if called outside an aggregate context or when used as a window function.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the execution context

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - AggState
  - AggStatePerAgg
  - AggStatePerTrans
- Called from (representative examples):
  - ordered_set_startup (src/backend/utils/adt/orderedsetaggs.c:142)

## Notes and Other Information
- Part of the public API exposed to aggregate functions for accessing aggregate metadata
- Returns NULL when called outside aggregate context or when used as a window function
- For merged aggregates, transition functions get indeterminate Aggref results but final functions get precise results
- Critical for ordered-set aggregates and other aggregate functions that need access to their Aggref configuration
- The returned Aggref contains information about the aggregate's arguments, sorting requirements, and other execution parameters
- Should not be used to access final-function-specific fields when called from transition functions due to aggregate merging