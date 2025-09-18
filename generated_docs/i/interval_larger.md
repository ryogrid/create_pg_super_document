# interval_larger

## Location
src/backend/utils/adt/timestamp.c: 3433 - 3446

## Overview
The `interval_larger` function implements the PostgreSQL `GREATEST` operation for interval data types, returning the larger of two interval values.

## Definition
```c
Datum interval_larger(PG_FUNCTION_ARGS)
```

## Detailed Description
The `interval_larger` function compares two interval values and returns the one that is chronologically longer or larger in magnitude. It uses the internal comparison function `interval_cmp_internal` to perform the comparison, ensuring consistency with PostgreSQL's interval comparison operations. The function follows PostgreSQL's standard function interface pattern and returns a pointer to one of the input intervals rather than creating a new interval object.

The comparison logic relies on `interval_cmp_internal` which returns a value greater than 0 if the first interval is larger than the second, 0 if they are equal, and less than 0 if the first interval is smaller. This function is the counterpart to `interval_smaller`.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `interval1`: First interval value to compare
  - `interval2`: Second interval value to compare

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INTERVAL_P` - Extract interval arguments from function call
  - [interval_cmp_internal](interval_cmp_internal.md) - Internal function for comparing intervals (located at src/backend/utils/adt/timestamp.c:2505-2513)
  - `PG_RETURN_INTERVAL_P` - Return the larger interval result
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL SQL function dispatch)

## Notes and Other Information
- Returns a pointer to one of the input intervals rather than allocating new memory
- Uses the same comparison mechanism as `interval_smaller` but with opposite logic (> 0 instead of < 0)
- Part of PostgreSQL's interval utility functions, typically used in GREATEST() SQL operations
- Efficient implementation that avoids unnecessary memory allocation by returning existing interval pointers
- Maintains consistency with PostgreSQL's interval comparison semantics through use of `interval_cmp_internal`