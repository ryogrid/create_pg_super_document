# int2gt

## Location
src/backend/utils/adt/int.c: 486 - 494

## Overview
Compares two 16-bit signed integers and returns true if the first is greater than the second.

## Definition
Datum int2gt(PG_FUNCTION_ARGS)

## Detailed Description
This function implements the greater-than comparison operator (>) for PostgreSQL's int2 (smallint) data type. It extracts two 16-bit signed integer arguments from the function call context and performs an arithmetic comparison, returning a boolean result. The function is part of PostgreSQL's built-in operator system and is typically invoked through SQL expressions using the > operator on smallint values.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: PostgreSQL function call context containing the arguments
  - arg1 (int16): First 16-bit signed integer argument (left operand)
  - arg2 (int16): Second 16-bit signed integer argument (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16: Extracts int16 arguments from function context
  - PG_RETURN_BOOL: Returns boolean result to PostgreSQL
- Called from (representative examples):
  - SQL queries using > operator on smallint columns
  - Sorting and ordering operations in queries

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:486-494
- Part of PostgreSQL's comprehensive integer comparison operator family
- Used internally by the query planner and executor for smallint comparisons
- Returns PostgreSQL Datum type for integration with the function call framework