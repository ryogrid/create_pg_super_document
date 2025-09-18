# interval_gt

## Location
[src/backend/utils/adt/timestamp.c:2550-2558](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2550-L2558)

## Overview
Compares two interval values to determine if the first is greater than the second, returning true if the first interval represents a longer time span.

## Definition


## Detailed Description
The `interval_gt` function is a PostgreSQL function that implements the greater-than operator (>) for interval data types. It extracts two interval arguments from the function call arguments using PostgreSQL's argument handling macros, then uses `interval_cmp_internal()` to perform the actual comparison. The function returns a boolean result indicating whether the first interval is greater than (longer than) the second interval.

## Parameters / Member Variables
- Uses PostgreSQL's `PG_FUNCTION_ARGS` macro system to receive arguments
- Argument 0: First interval for comparison (left operand)
- Argument 1: Second interval for comparison (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INTERVAL_P (macro for extracting interval arguments)
  - [interval_cmp_internal](interval_cmp_internal.md)
  - PG_RETURN_BOOL (macro for returning boolean results)
- Called from:
  - No direct references found (likely registered as SQL operator function)

## Notes and Other Information
- This function implements the SQL `>` operator for interval types
- Returns `Datum` type following PostgreSQL's function calling convention
- Uses PostgreSQL's internal function argument and return value macros
- The actual comparison logic is delegated to `interval_cmp_internal()`
- Registered in the PostgreSQL system catalogs to handle interval greater-than operations in SQL queries
- Part of a complete set of comparison operators for intervals (=, !=, <, >, <=, >=)
- Used for ordering intervals in sorting operations and range comparisons
- Complementary to `interval_lt` - returns true when the first argument is longer than the second