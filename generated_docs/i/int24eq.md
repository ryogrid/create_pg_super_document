# int24eq

## Location
src/backend/utils/adt/int.c: 504 - 512

## Overview
Compares a 16-bit signed integer with a 32-bit signed integer and returns true if they are equal.

## Definition
Datum int24eq(PG_FUNCTION_ARGS)

## Detailed Description
This function implements the equality comparison operator (=) between PostgreSQL's int2 (smallint) and int4 (integer) data types. It extracts a 16-bit signed integer and a 32-bit signed integer from the function call context, performs an equality comparison with implicit type promotion, and returns a boolean result. This cross-type comparison function enables PostgreSQL to compare smallint and integer values directly without explicit casting.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: PostgreSQL function call context containing the arguments
  - arg1 (int16): First argument as 16-bit signed integer (smallint)
  - arg2 (int32): Second argument as 32-bit signed integer (integer)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16: Extracts int16 argument from function context
  - PG_GETARG_INT32: Extracts int32 argument from function context
  - PG_RETURN_BOOL: Returns boolean result to PostgreSQL
- Called from (representative examples):
  - SQL queries comparing smallint and integer columns
  - Mixed-type comparisons in WHERE clauses and JOIN conditions

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:504-512
- Enables cross-type comparison between int2 and int4 without explicit casting
- The int16 value is implicitly promoted to int32 for comparison
- Part of PostgreSQL's type coercion system for numeric comparisons
- Returns PostgreSQL Datum type for integration with the function call framework