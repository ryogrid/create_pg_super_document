# int2lt

## Location
[src/backend/utils/adt/int.c:468-476](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L468-L476)

## Overview
Compares two 16-bit signed integers and returns true if the first is less than the second.

## Definition

```c
Datum
int2lt(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the less-than comparison operator (<) for PostgreSQL's int2 (smallint) data type. It extracts two 16-bit signed integer arguments from the function call context and performs a simple arithmetic comparison, returning a boolean result. The function is part of PostgreSQL's built-in operator system and is typically invoked through SQL expressions using the < operator on smallint values.

## Parameters / Member Variables
- : PostgreSQL function call context containing the arguments
  -  (int16): First 16-bit signed integer argument (left operand)
  -  (int16): Second 16-bit signed integer argument (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts int16 arguments from function context
  - : Returns boolean result to PostgreSQL
- Called from (representative examples):
  - SQL queries using < operator on smallint columns
  - Index and sorting operations requiring comparison

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:468-476
- Part of PostgreSQL's comprehensive integer comparison operator family
- Used internally by the query planner and executor for smallint comparisons
- Returns PostgreSQL Datum type for integration with the function call framework