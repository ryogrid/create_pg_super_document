# btfloat84cmp

## Location
[src/backend/utils/adt/float.c:1004-1019](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1004-L1019)

## Overview
PostgreSQL function that compares a double-precision floating-point number (float8) with a single-precision floating-point number (float4) for B-tree indexing operations.

## Definition

```c
Datum
btfloat84cmp(PG_FUNCTION_ARGS)
```
## Detailed Description
This function performs a three-way comparison between a float8 and a float4 value by widening the float4 to float8 precision and then using the standard float8_cmp_internal comparison function. This is the complement to btfloat48cmp, handling the case where the first argument is double-precision and the second is single-precision. The function enables mixed-precision comparisons in B-tree operations, supporting indexes and sorting operations that involve both single and double precision floating-point values. It follows PostgreSQL's standard comparison convention of returning -1, 0, or 1.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: Double-precision floating-point number (float8)
  - Argument 1: Single-precision floating-point number (float4)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (macro for extracting float8 arguments)
  - PG_GETARG_FLOAT4 (macro for extracting float4 arguments)
  - [float8_cmp_internal](../f/float8_cmp_internal.md) (performs the actual comparison after type promotion)
  - PG_RETURN_INT32 (macro for returning int32 result)

- Called from (representative examples):
  - No direct references found in the codebase (likely referenced through function pointer tables or system catalogs)

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:1004-1019
- Complement function to btfloat48cmp, handling the reverse argument order (float8, float4)
- The function widens the float4 argument to float8 precision before comparison, ensuring consistent precision handling
- This enables comprehensive mixed-precision arithmetic and comparison operations in PostgreSQL
- Used in B-tree indexing scenarios where float8 and float4 values need to be compared
- The type promotion from float4 to float8 is implicit and handled by the C type system
- Returns an int32 value following PostgreSQL's comparison function convention (-1, 0, 1)