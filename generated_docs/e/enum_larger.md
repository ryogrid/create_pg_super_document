# enum_larger

## Location
src/backend/utils/adt/enum.c: 369 - 377

## Overview
PostgreSQL function that returns the larger of two enum values by comparing their defined ordering and returning the enum value that appears later in the type definition.

## Definition
```c
Datum enum_larger(PG_FUNCTION_ARGS)
```

## Detailed Description
The `enum_larger` function implements a max-like operation for PostgreSQL enum data types. Unlike the boolean comparison operators, this function returns one of the actual input enum values rather than a boolean result. It extracts two enum OID arguments and uses `enum_cmp_internal` to determine their relative ordering. If the first enum value comes after the second in the type's defined sequence (comparison result > 0), it returns the first value; otherwise, it returns the second value.

This function serves as the implementation for aggregate functions like MAX() when applied to enum columns, and can be used in SQL expressions where you need to select the "larger" (later in declaration order) of two enum values.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - First argument (index 0): OID of the first enum value to compare
  - Second argument (index 1): OID of the second enum value to compare

## Dependencies
- Functions called/Symbols referenced:
  - [enum_cmp_internal](enum_cmp_internal.md): Core enum comparison function that performs the actual comparison logic
  - PG_GETARG_OID: Macro to extract OID arguments from function call
  - PG_RETURN_OID: Macro to return an OID value as the result
- Called from (representative examples):
  - MAX() aggregate functions on enum columns
  - GREATEST() SQL function with enum arguments
  - Custom SQL functions requiring maximum enum selection

## Notes and Other Information
- Returns the actual enum value (as an OID) rather than a boolean comparison result
- Uses the same comparison logic as other enum operators through `enum_cmp_internal`
- The "larger" enum value is determined by the declaration order in the CREATE TYPE statement
- Part of PostgreSQL's support for aggregate operations on enum types
- Essential for implementing proper MAX() behavior on enum columns in GROUP BY operations
- Complementary to `enum_smaller`, using strict inequality (> 0) to select the later-declared enum value
- The function follows PostgreSQL's fmgr (function manager) calling convention