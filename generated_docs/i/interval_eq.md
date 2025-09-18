# interval_eq

## Location
src/backend/utils/adt/timestamp.c: 2523 - 2531

## Overview
Compares two interval values for equality, returning true if they represent the same time span.

## Definition


## Detailed Description
The `interval_eq` function is a PostgreSQL function that implements the equality operator (=) for interval data types. It extracts two interval arguments from the function call arguments using PostgreSQL's argument handling macros, then uses `interval_cmp_internal()` to perform the actual comparison. The function returns a boolean result indicating whether the two intervals are equal (represent exactly the same time span).

## Parameters / Member Variables
- Uses PostgreSQL's `PG_FUNCTION_ARGS` macro system to receive arguments
- Argument 0: First interval for comparison
- Argument 1: Second interval for comparison

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INTERVAL_P (macro for extracting interval arguments)
  - [interval_cmp_internal](interval_cmp_internal.md)
  - PG_RETURN_BOOL (macro for returning boolean results)
- Called from:
  - No direct references found (likely registered as SQL operator function)

## Notes and Other Information
- This function implements the SQL `=` operator for interval types
- Returns `Datum` type following PostgreSQL's function calling convention
- Uses PostgreSQL's internal function argument and return value macros
- The actual comparison logic is delegated to `interval_cmp_internal()`
- Registered in the PostgreSQL system catalogs to handle interval equality operations in SQL queries