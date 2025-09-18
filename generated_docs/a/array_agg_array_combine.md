# array_agg_array_combine

## Location
[src/backend/utils/adt/array_userfuncs.c:901-1049](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L901-L1049)

## Overview
Combines two ArrayBuildStateArr states during parallel aggregation of array_agg operations, merging accumulated arrays from different worker processes.

## Definition
Datum array_agg_array_combine(PG_FUNCTION_ARGS)

## Detailed Description
This function is used as a combine function for the array_agg aggregate when running in parallel mode. It takes two ArrayBuildStateArr states (representing partial aggregation results from different parallel workers) and combines them into a single state. The function handles memory management by ensuring all data is moved to the aggregation context, validates that the arrays being combined have compatible dimensions, and efficiently merges the data and null bitmaps.

The function implements the following logic:
- If either state is NULL, returns the non-NULL state (or NULL if both are NULL)
- If state1 is NULL but state2 has data, copies state2's data into the aggregation context
- If both states have data, validates dimensional compatibility and merges them by concatenating data, combining null bitmaps, and updating dimension information

## Parameters / Member Variables
- : Function call information structure containing the two ArrayBuildStateArr pointers as arguments
- Returns: Combined ArrayBuildStateArr state as a Datum pointer

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - initArrayResultArr
  - [array_bitmap_copy](array_bitmap_copy.md)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md)
  - [repalloc](../r/repalloc.md)
  - [palloc](../p/palloc.md)
  - memcpy
- Called from (representative examples):
  - No direct references found (used as aggregate combine function)

## Notes and Other Information
- This function is specifically designed for parallel aggregation of array_agg
- Ensures dimensional compatibility by checking that all dimensions except the first match exactly
- Uses power-of-2 allocation strategy for efficient memory management
- Handles null bitmap management for proper NULL value tracking in arrays
- All memory allocations are done in the aggregation context to ensure proper cleanup