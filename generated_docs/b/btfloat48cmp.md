# btfloat48cmp

## Location
src/backend/utils/adt/float.c: 994 - 1003

## Overview
PostgreSQL function that compares a single-precision floating-point number (float4) with a double-precision floating-point number (float8) for B-tree indexing operations.

## Definition


## Detailed Description
This function performs a three-way comparison between a float4 and a float8 value by widening the float4 to float8 precision and then using the standard float8_cmp_internal comparison function. This allows for mixed-precision comparisons in B-tree operations, enabling indexes and sorting operations that involve both single and double precision floating-point values. The function follows PostgreSQL's standard comparison convention of returning -1, 0, or 1 for less than, equal to, or greater than relationships.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: Single-precision floating-point number (float4)
  - Argument 1: Double-precision floating-point number (float8)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4 (macro for extracting float4 arguments)
  - PG_GETARG_FLOAT8 (macro for extracting float8 arguments)
  - float8_cmp_internal (performs the actual comparison after type promotion)
  - PG_RETURN_INT32 (macro for returning int32 result)

- Called from (representative examples):
  - No direct references found in the codebase (likely referenced through function pointer tables or system catalogs)

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:994-1003
- The function widens the float4 argument to float8 precision before comparison, ensuring consistent precision handling
- This enables mixed-precision arithmetic and comparison operations in PostgreSQL
- Used in B-tree indexing scenarios where float4 and float8 values need to be compared
- The type promotion from float4 to float8 is implicit and handled by the C type system
- Returns an int32 value following PostgreSQL's comparison function convention (-1, 0, 1)