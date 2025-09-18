# int84ge

## Location
src/backend/utils/adt/int8.c: 215 - 226

## Overview
This function compares an 8-byte (int64) integer with a 4-byte (int32) integer and returns true if the 8-byte integer is greater than or equal to the 4-byte integer.

## Definition


## Detailed Description
The int84ge function is a PostgreSQL built-in function that performs a "greater than or equal to" comparison between an 8-byte integer (int64/bigint) and a 4-byte integer (int32/integer). It follows PostgreSQL's function calling convention using the PG_FUNCTION_ARGS macro. The function extracts the two arguments from the function call context, performs the comparison, and returns a boolean result using PostgreSQL's Datum system.

## Parameters / Member Variables
- : PostgreSQL function calling convention macro that provides access to function arguments
  - First argument (index 0): int64 value to compare
  - Second argument (index 1): int32 value to compare against

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64: Extracts 8-byte integer from function arguments
  - PG_GETARG_INT32: Extracts 4-byte integer from function arguments
  - PG_RETURN_BOOL: Returns boolean result as PostgreSQL Datum
- Called from:
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/int8.c at lines 215-226
- This function is part of PostgreSQL's arithmetic and comparison operators for mixed integer types
- The comparison is performed directly using C's >= operator after extracting the values
- The int32 value is implicitly promoted to int64 during the comparison