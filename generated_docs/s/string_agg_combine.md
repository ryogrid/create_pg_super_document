# string_agg_combine

## Location
[src/backend/utils/adt/varlena.c:5241-5290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L5241-L5290)

## Overview
The combine function for PostgreSQL's string_agg() aggregate that merges two partial aggregation states in parallel query execution.

## Definition

```c
Datum
string_agg_combine(PG_FUNCTION_ARGS)
```
## Detailed Description
The string_agg_combine function serves as the combine function for the string_agg aggregate, specifically designed for parallel query execution. It merges two StringInfo states representing partial aggregation results from different parallel workers or execution contexts.

Key behavioral aspects:
- Handles various NULL state combinations gracefully
- When state1 is NULL, creates a new state and copies state2's data into the aggregate context
- When state2 is NULL, simply returns state1 (already in correct context)
- When both states exist, appends state2's data to state1
- Preserves the cursor field (first delimiter length) from the appropriate state
- Ensures the result is always in the aggregate memory context

The function is critical for PostgreSQL's parallel aggregation infrastructure, allowing partial string_agg results to be efficiently combined.

## Parameters / Member Variables
- : Standard PostgreSQL function call information containing:
  - Arg 0: First StringInfo state (may be NULL)
  - Arg 1: Second StringInfo state (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md) (validates aggregate execution context)
  - [makeStringAggState](../m/makeStringAggState.md) (creates new StringInfo state in aggregate context)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md) (appends binary data to StringInfo buffer)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (changes memory allocation context)
  - PG_ARGISNULL, PG_GETARG_POINTER (PostgreSQL argument macros)

- Called from:
  - PostgreSQL parallel aggregate execution framework (not directly referenced in source)

## Notes and Other Information
- Must be called within an aggregate context (enforced by AggCheckCallContext)
- Designed specifically for parallel query execution and partial aggregation combining
- Handles memory context switching to ensure results are in the correct aggregate context
- The cursor field preservation ensures proper delimiter handling in the final function
- Efficient binary data copying using appendBinaryStringInfo for raw string data
- Works for both text and bytea variants of string_agg