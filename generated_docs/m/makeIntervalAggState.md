# makeIntervalAggState

## Location
src/backend/utils/adt/timestamp.c: 3926 - 3947

## Overview
Creates and initializes an IntervalAggState structure in the aggregate function's memory context for interval aggregate operations that need to track sum and count.

## Definition
```c
static IntervalAggState *makeIntervalAggState(FunctionCallInfo fcinfo)
```

## Detailed Description
This static function is a utility for interval aggregate functions that need to maintain state across multiple input rows. It allocates and zero-initializes an IntervalAggState structure in the appropriate memory context for aggregate functions. The function ensures proper memory management by switching to the aggregate's memory context before allocation, which ensures the state persists for the lifetime of the aggregate operation.

The function performs essential validation by checking that it's being called within a proper aggregate context using AggCheckCallContext. This prevents misuse of the function outside of aggregate operations and ensures proper memory context handling.

## Parameters / Member Variables
- `fcinfo`: Function call information containing context and parameters for the aggregate function

## Dependencies
- Functions called/Symbols referenced:
  - AggCheckCallContext
  - MemoryContextSwitchTo
  - palloc0
  - elog
  - IntervalAggState (type)
  - FunctionCallInfo (type)
- Called from:
  - interval_avg_accum (in src/backend/utils/adt/timestamp.c:4010)
  - interval_avg_combine (in src/backend/utils/adt/timestamp.c:4039)

## Notes and Other Information
- Static function used internally by interval aggregate functions
- Ensures state allocation occurs in the aggregate's memory context for proper lifetime management
- Alternative to direct palloc0 usage when aggregate context allocation is required
- Used specifically for aggregate functions that need to compute both sum and count (e.g., AVG)
- Performs validation to prevent misuse outside aggregate contexts
- Returns a zero-initialized IntervalAggState structure ready for use