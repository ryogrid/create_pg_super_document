# int28gt

## Location
[src/backend/utils/adt/int8.c:368-376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L368-L376)

## Overview
Compares a 16-bit integer (int2) with a 64-bit integer (int8) and returns true if the int2 value is greater than the int8 value.

## Definition
Datum int28gt(PG_FUNCTION_ARGS)

## Detailed Description
This function implements the greater-than comparison operator for mixed-type comparison between int2 (16-bit integer) and int8 (64-bit integer) data types. It extracts the int2 value from the first argument and the int8 value from the second argument, performs a direct numerical comparison, and returns a boolean result indicating whether the first value is greater than the second.

## Parameters / Member Variables
- First argument (position 0): int2 value (16-bit signed integer) to be compared
- Second argument (position 1): int8 value (64-bit signed integer) to compare against

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16 (macro to extract int2 argument)
  - PG_GETARG_INT64 (macro to extract int8 argument)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's type system support for cross-type comparisons
- The comparison is performed using C's native integer comparison, which handles the type promotion from int16 to int64 automatically
- Located in src/backend/utils/adt/int8.c:368-376
- Returns a Datum containing a boolean value indicating the comparison result