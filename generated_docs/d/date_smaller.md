# date_smaller

## Location
[src/backend/utils/adt/date.c:476-486](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L476-L486)

## Overview
Returns the smaller (earlier) of two date values.

## Definition
```c
Datum date_smaller(PG_FUNCTION_ARGS)
```

## Detailed Description
The `date_smaller` function compares two DateADT values and returns the smaller one. This is a PostgreSQL built-in function that implements the minimum operation for date types. The function performs a simple comparison between the two input dates and returns whichever date represents an earlier point in time. This function is the counterpart to `date_larger` and is part of PostgreSQL's date comparison and utility functions.

Like `date_larger`, the comparison is done directly on the DateADT values since they are stored as integers representing days since a reference point, making direct comparison valid for determining chronological order.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - First argument (index 0): `DateADT` - The first date value to compare
  - Second argument (index 1): `DateADT` - The second date value to compare

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_DATEADT` - Macro to extract DateADT arguments from function call
  - `PG_RETURN_DATEADT` - Macro to return a DateADT result
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- This function implements the SQL LEAST() functionality for date types
- Returns the chronologically earlier date of the two inputs
- Part of the PostgreSQL date comparison function family located in src/backend/utils/adt/date.c
- The comparison works correctly with infinite date values due to their internal representation
- Can be used in SQL as a built-in function for finding minimum dates
- Complements the `date_larger` function for complete min/max date operations