# qsort_tuple_int32_compare

## Location
[src/backend/utils/sort/tuplesort.c:545-574](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L545-L574)

## Overview
A specialized inline comparison function for sorting tuples when the first sort key uses 32-bit integer comparison, designed to optimize sorting performance for int32 data types.

## Definition
```c
static pg_attribute_always_inline int
qsort_tuple_int32_compare(SortTuple *a, SortTuple *b, Tuplesortstate *state)
```

## Detailed Description
This function is a specialized comparator in PostgreSQL's tuple sorting optimization system, specifically tailored for sorting tuples where the primary sort key is a 32-bit integer type. It uses `ApplyInt32SortComparator` to perform the comparison, which is optimized for 32-bit integer values. Like its sibling functions, it first compares the primary sort key values, and if they are equal, it delegates to the tiebreak function for comparing additional sort keys (unless only one sort key exists).

The function is marked with `pg_attribute_always_inline` to ensure compiler inlining, eliminating function call overhead during sorting operations. This is particularly beneficial for int32 columns which are very common in database operations.

## Parameters / Member Variables
- `a`: Pointer to the first SortTuple to compare
- `b`: Pointer to the second SortTuple to compare  
- `state`: Pointer to the Tuplesortstate containing sort configuration and callback functions

## Dependencies
- Functions called/Symbols referenced:
  - [ApplyInt32SortComparator](../A/ApplyInt32SortComparator.md) - Performs the actual 32-bit integer comparison of datum values
  - `[state](../s/state.md)->base.comparetup_tiebreak` - Fallback function for comparing additional sort keys
  - `SortTuple` - Structure representing a tuple being sorted
  - `[Tuplesortstate](../T/Tuplesortstate.md)` - State structure containing sort configuration
- Called from (representative examples):
  - Used as an inline comparator in specialized sorting routines (no direct references found)

## Notes and Other Information
- Part of the family of specialized comparators for optimizing common sorting scenarios
- Specifically optimized for 32-bit integer data types (int4 in PostgreSQL)
- Avoids the tiebreak function call when there's only one sort key (`state->base.onlyKey != NULL`)
- Returns standard comparison result: 0 for equal tuples, negative for a < b, positive for a > b
- The inline attribute ensures maximum performance by eliminating function call overhead
- Complements other specialized comparators to handle different numeric data types efficiently
- Common use case for sorting by integer primary keys, counts, and other int4 columns