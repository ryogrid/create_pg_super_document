# int28eq

## Location
[src/backend/utils/adt/int8.c:341-349](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L341-L349)

## Overview
A PostgreSQL built-in function that compares a 2-byte integer (smallint/int16) with an 8-byte integer (bigint/int64) to determine if they are equal.

## Definition
Datum int28eq(PG_FUNCTION_ARGS)

## Detailed Description
The int28eq function implements the equality comparison operator between a 2-byte integer and an 8-byte integer. This function is part of PostgreSQL system for handling mixed-precision integer comparisons in the reverse order from the int82 family. It extracts the int16 value from the first argument and the int64 value from the second argument, performs a direct comparison, and returns a boolean result indicating whether the values are equal.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: PostgreSQL function argument structure containing:
  - Argument 0: int16 value (2-byte integer)
  - Argument 1: int64 value (8-byte integer)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16
  - PG_GETARG_INT64
  - PG_RETURN_BOOL
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL cross-type comparison operators for the int28 family
- Handles automatic type promotion from int16 to int64 for comparison
- Returns true if the int16 value equals the int64 value, false otherwise
- Comment in source indicates this is part of int28relop family for 16-bit val1 relop 64-bit val2
- Located in src/backend/utils/adt/int8.c, which contains various int8 (bigint) operations