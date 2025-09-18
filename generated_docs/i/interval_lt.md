# interval_lt

## Location
src/backend/utils/adt/timestamp.c: 2541 - 2549

## Overview
Compares two interval values to determine if the first is less than the second, returning true if the first interval represents a shorter time span.

## Definition


## Detailed Description
The `interval_lt` function is a PostgreSQL function that implements the less-than operator (<) for interval data types. It extracts two interval arguments from the function call arguments using PostgreSQL's argument handling macros, then uses `interval_cmp_internal()` to perform the actual comparison. The function returns a boolean result indicating whether the first interval is less than (shorter than) the second interval.

## Parameters / Member Variables
- Uses PostgreSQL's `PG_FUNCTION_ARGS` macro system to receive arguments
- Argument 0: First interval for comparison (left operand)
- Argument 1: Second interval for comparison (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INTERVAL_P (macro for extracting interval arguments)
  - interval_cmp_internal
  - PG_RETURN_BOOL (macro for returning boolean results)
- Called from:
  - No direct references found (likely registered as SQL operator function)

## Notes and Other Information
- This function implements the SQL `<` operator for interval types
- Returns `Datum` type following PostgreSQL's function calling convention
- Uses PostgreSQL's internal function argument and return value macros
- The actual comparison logic is delegated to `interval_cmp_internal()`
- Registered in the PostgreSQL system catalogs to handle interval less-than operations in SQL queries
- Part of a complete set of comparison operators for intervals (=, !=, <, >, <=, >=)
- Used for ordering intervals in sorting operations and range comparisons