# interval_ne

## Location
[src/backend/utils/adt/timestamp.c:2532-2540](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2532-L2540)

## Overview
Compares two interval values for inequality, returning true if they represent different time spans.

## Definition

```c
Datum
interval_ne(PG_FUNCTION_ARGS)
```
## Detailed Description
The `interval_ne` function is a PostgreSQL function that implements the inequality operator (!=, <>) for interval data types. It extracts two interval arguments from the function call arguments using PostgreSQL's argument handling macros, then uses `interval_cmp_internal()` to perform the actual comparison. The function returns a boolean result indicating whether the two intervals are not equal (represent different time spans).

## Parameters / Member Variables
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
- This function implements the SQL `!=` and `<>` operators for interval types
- Returns `Datum` type following PostgreSQL's function calling convention
- Uses PostgreSQL's internal function argument and return value macros
- The actual comparison logic is delegated to `interval_cmp_internal()`
- Registered in the PostgreSQL system catalogs to handle interval inequality operations in SQL queries
- Complementary to `interval_eq` - returns the opposite boolean result

## Simplified Source

```c
Datum interval_ne(PG_FUNCTION_ARGS) {
    // Extract two interval arguments
    Interval *interval1 = PG_GETARG_INTERVAL_P(0);
    Interval *interval2 = PG_GETARG_INTERVAL_P(1);

    // Compare intervals and return true if not equal
    PG_RETURN_BOOL(interval_cmp_internal(interval1, interval2) != 0);
}
```