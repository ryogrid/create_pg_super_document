# multirange_empty

## Location
[src/backend/utils/adt/multirangetypes.c:1556-1564](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L1556-L1564)

## Overview
Tests whether a multirange is empty, returning true if the multirange contains no ranges or all ranges within it are empty.

## Definition
```c
Datum multirange_empty(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a simple boolean test to determine if a multirange is empty. It serves as a SQL-callable wrapper around the internal MultirangeIsEmpty macro/function. An empty multirange is one that contains no valid ranges or where all contained ranges are themselves empty, meaning the multirange represents no actual values or intervals.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention containing:
  - Arg 0: Input multirange (MultirangeType) to test for emptiness

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MULTIRANGE_P
  - MultirangeIsEmpty
  - PG_RETURN_BOOL
  - MultirangeType
- Called from (representative examples):
  - No direct references found (likely used via SQL function calls)

## Notes and Other Information
- This is a very lightweight function that simply wraps the MultirangeIsEmpty check for SQL accessibility
- Returns a boolean value: true for empty multiranges, false for non-empty ones
- Empty multiranges can result from various operations like intersecting disjoint ranges
- This function is part of the multirange -> bool family of functions as noted in the source comments
- Essential for conditional logic and filtering operations on multirange data in SQL queries
- The emptiness test is fundamental to many multirange operations and validations