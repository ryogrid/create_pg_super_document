# array_agg_combine

## Location
[src/backend/utils/adt/array_userfuncs.c:525-621](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L525-L621)

## Overview
Combines two ArrayBuildState structures during parallel aggregate processing for array_agg(), merging their accumulated elements into a single state.

## Definition


## Detailed Description
This function is a combine function used in parallel aggregation for the array_agg() aggregate function. It merges two ArrayBuildState structures (state1 and state2) that have been accumulated in different parallel workers into a single combined state. The function handles various scenarios including NULL states, empty states, and the need to expand arrays when combining states with different numbers of elements.

The function ensures proper memory management by copying data into the aggregate context and uses efficient memory allocation strategies (power of 2 sizing) when expanding arrays. It preserves both the actual data values and their null status indicators during the merge process.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : First ArrayBuildState to combine (may be NULL)
  - : Second ArrayBuildState to combine (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - [ArrayBuildState](../A/ArrayBuildState.md)
  - [initArrayResultWithSize](../i/initArrayResultWithSize.md)
  - [datumCopy](../d/datumCopy.md)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md)
  - [repalloc](../r/repalloc.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - memcpy
- Called from (representative examples):
  - PostgreSQL parallel aggregate framework (internal)

## Notes and Other Information
- This is specifically designed for parallel aggregation support in PostgreSQL
- Handles memory context switching to ensure data persists in the correct aggregate context
- Uses power-of-2 allocation strategy for efficient array growth
- Preserves element type consistency between states being combined
- Returns NULL only when both input states are NULL
- Essential for scaling array_agg() operations across multiple parallel workers