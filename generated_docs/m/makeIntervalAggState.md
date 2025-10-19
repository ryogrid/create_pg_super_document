# makeIntervalAggState

## Location
[src/backend/utils/adt/timestamp.c:3926-3947](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L3926-L3947)

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
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc0](../p/palloc0.md)
  - elog
  - [IntervalAggState](../I/IntervalAggState.md) (type)
  - [FunctionCallInfo](../F/FunctionCallInfo.md) (type)
- Called from:
  - [interval_avg_accum](../i/interval_avg_accum.md) (in src/backend/utils/adt/timestamp.c:4010)
  - [interval_avg_combine](../i/interval_avg_combine.md) (in src/backend/utils/adt/timestamp.c:4039)

## Notes and Other Information
- Static function used internally by interval aggregate functions
- Ensures state allocation occurs in the aggregate's memory context for proper lifetime management
- Alternative to direct palloc0 usage when aggregate context allocation is required
- Used specifically for aggregate functions that need to compute both sum and count (e.g., AVG)
- Performs validation to prevent misuse outside aggregate contexts
- Returns a zero-initialized IntervalAggState structure ready for use

## Simplified Source

```c
static IntervalAggState *
makeIntervalAggState(FunctionCallInfo fcinfo)
{
    IntervalAggState *state;
    MemoryContext agg_context;
    MemoryContext old_context;

    // Validate aggregate context and get memory context
    if (!AggCheckCallContext(fcinfo, &agg_context))
        elog(ERROR, "aggregate function called in non-aggregate context");

    // Switch to aggregate context for allocation
    old_context = MemoryContextSwitchTo(agg_context);

    // Allocate and zero-initialize state structure
    state = (IntervalAggState *) palloc0(sizeof(IntervalAggState));

    // Restore original memory context
    MemoryContextSwitchTo(old_context);

    return state;
}
```