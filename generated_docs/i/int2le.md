# int2le

## Location
[src/backend/utils/adt/int.c:477-485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L477-L485)

## Overview
Compares two 16-bit signed integers and returns true if the first is less than or equal to the second.

## Definition
Datum int2le(PG_FUNCTION_ARGS)

## Detailed Description
This function implements the less-than-or-equal-to comparison operator (<=) for PostgreSQL's int2 (smallint) data type. It extracts two 16-bit signed integer arguments from the function call context and performs an arithmetic comparison, returning a boolean result. The function is part of PostgreSQL's built-in operator system and is typically invoked through SQL expressions using the <= operator on smallint values.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: PostgreSQL function call context containing the arguments
  - arg1 (int16): First 16-bit signed integer argument (left operand)
  - arg2 (int16): Second 16-bit signed integer argument (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16: Extracts int16 arguments from function context
  - PG_RETURN_BOOL: Returns boolean result to PostgreSQL
- Called from (representative examples):
  - SQL queries using <= operator on smallint columns
  - [Range](../R/Range.md) and boundary condition checks in queries

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:477-485
- Part of PostgreSQL's comprehensive integer comparison operator family
- Used internally by the query planner and executor for smallint comparisons
- Returns PostgreSQL Datum type for integration with the function call framework

## Simplified Source

```c
Datum int2le(PG_FUNCTION_ARGS) {
    // Extract two 16-bit integer arguments
    int16 first_value = PG_GETARG_INT16(0);
    int16 second_value = PG_GETARG_INT16(1);

    // Return true if first <= second, false otherwise
    PG_RETURN_BOOL(first_value <= second_value);
}
```