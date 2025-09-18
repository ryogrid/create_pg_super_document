# interval_smaller

## Location
src/backend/utils/adt/timestamp.c: 3418 - 3432

## Overview
The `interval_smaller` function implements the PostgreSQL `LEAST` operation for interval data types, returning the smaller of two interval values.

## Definition
```c
Datum interval_smaller(PG_FUNCTION_ARGS)
```

## Detailed Description
The `interval_smaller` function compares two interval values and returns the one that is chronologically shorter or smaller in magnitude. It uses the internal comparison function `interval_cmp_internal` to perform the comparison, ensuring consistency with PostgreSQL's interval comparison operations. The function follows PostgreSQL's standard function interface pattern and returns a pointer to one of the input intervals rather than creating a new interval object.

The comparison logic relies on `interval_cmp_internal` which returns a value less than 0 if the first interval is smaller than the second, 0 if they are equal, and greater than 0 if the first interval is larger.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `interval1`: First interval value to compare
  - `interval2`: Second interval value to compare

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INTERVAL_P` - Extract interval arguments from function call
  - [interval_cmp_internal](interval_cmp_internal.md) - Internal function for comparing intervals (located at src/backend/utils/adt/timestamp.c:2505-2513)
  - `PG_RETURN_INTERVAL_P` - Return the smaller interval result
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL SQL function dispatch)

## Notes and Other Information
- Returns a pointer to one of the input intervals rather than allocating new memory
- Explicitly uses `interval_cmp_internal` to ensure consistency with interval comparison operations
- Part of PostgreSQL's interval utility functions, typically used in LEAST() SQL operations
- The comment in the source emphasizes the importance of using the internal comparison function to maintain agreement with comparison operations
- Efficient implementation that avoids unnecessary memory allocation by returning existing interval pointers