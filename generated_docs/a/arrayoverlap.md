# arrayoverlap

## Location
[src/backend/utils/adt/arrayfuncs.c:4512-4529](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L4512-L4529)

## Overview
PostgreSQL function implementing the array overlap operator (&&) that determines whether two arrays have any elements in common.

## Definition

```c
Datum
arrayoverlap(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the PostgreSQL array overlap operator (&&), which returns true if the two input arrays share at least one common element. It serves as a thin wrapper around the  function, calling it with  to perform overlap detection rather than full containment checking.

This function is the SQL-callable interface for the array overlap operation, handling argument extraction, memory management, and result formatting. The actual comparison logic is delegated to , which efficiently determines if any element from the first array exists in the second array.

The function properly manages memory by freeing any detoasted copies of the input arrays to prevent memory leaks when dealing with large or compressed arrays.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
## Dependencies
- Functions called/Symbols referenced:
  -  - Extract array arguments from function call
  -  - Get collation for element comparisons
  -  - Core comparison logic with matchall=false
  -  - Free detoasted array copies
  -  - Return boolean result

- Called from (representative examples):
  - SQL queries using the && operator (e.g., )
  - Array overlap conditions in WHERE clauses and JOIN conditions
  - GIN and GiST index operations for array overlap searches

## Notes and Other Information
- Returns true if arrays have at least one element in common, false otherwise
- Implements the PostgreSQL && operator for arrays
- Requires arrays to have the same element type (enforced by )
- Uses efficient comparison by calling  with 
- Handles NULL elements according to  semantics (NULLs don't match)
- Supports all array types that have equality operators defined
- Memory-safe with proper cleanup of toasted array copies
- Performance depends on array sizes and element distribution
- Can benefit from GIN or GiST indexes when used in WHERE clauses
- Commonly used in applications dealing with tag arrays, category arrays, or any multi-valued attributes