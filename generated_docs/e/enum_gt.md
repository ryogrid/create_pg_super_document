# enum_gt

## Location
src/backend/utils/adt/enum.c: 351 - 359

## Overview
PostgreSQL function that implements the greater-than comparison operator for enum data types, returning true if the first enum value is strictly greater than the second.

## Definition
```c
Datum enum_gt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `enum_gt` function serves as the implementation for the > operator when comparing two PostgreSQL enum values. It extracts two enum OID arguments from the function call arguments and delegates the actual comparison logic to the internal `enum_cmp_internal` function. The function returns true only if the first enum value has a strictly greater sort order compared to the second enum value, based on the enum type's defined ordering.

This function is part of PostgreSQL's type system infrastructure, specifically designed to support SQL operations involving enum types. The comparison follows the logical ordering defined when the enum type was created, where enum values are ordered according to their declaration sequence in the CREATE TYPE statement.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - First argument (index 0): OID of the first enum value to compare
  - Second argument (index 1): OID of the second enum value to compare

## Dependencies
- Functions called/Symbols referenced:
  - enum_cmp_internal: Core enum comparison function that performs the actual comparison logic
  - PG_GETARG_OID: Macro to extract OID arguments from function call
  - PG_RETURN_BOOL: Macro to return boolean result
- Called from (representative examples):
  - SQL > operator expressions involving enum types
  - Index operations and sorting algorithms for enum columns

## Notes and Other Information
- This function implements one of the standard comparison operators required for a complete PostgreSQL data type
- The actual comparison logic is centralized in `enum_cmp_internal` to ensure consistency across all enum comparison operations
- Returns a PostgreSQL Datum containing a boolean value
- Part of the enum type's operator class, enabling use in indexes, sorting, and range operations
- The function follows PostgreSQL's fmgr (function manager) calling convention
- Differs from `enum_ge` by using strict inequality (> 0) rather than greater-or-equal (\>= 0)