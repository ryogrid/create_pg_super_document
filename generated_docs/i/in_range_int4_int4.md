# in_range_int4_int4

## Location
src/backend/utils/adt/int.c: 623 - 656

## Overview
A PostgreSQL function that determines whether a given int4 value falls within a range defined by a base value and an offset, supporting window function range frame calculations.

## Definition
```c
Datum in_range_int4_int4(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements range-based comparisons for PostgreSQL window functions using int4 data types. It checks whether a value is within a specified range from a base point, with the range defined by an offset. The function supports both addition and subtraction operations (controlled by the 'sub' parameter) and can perform either less-than-or-equal or greater-than-or-equal comparisons (controlled by the 'less' parameter). It includes overflow detection using pg_add_s32_overflow and provides appropriate fallback logic when overflow occurs.

## Parameters / Member Variables
- `val`: The int4 value to test against the range
- `base`: The int4 base value that defines the center of the range
- `offset`: The int4 offset value that defines the range size (must be non-negative)
- `sub`: Boolean flag indicating whether to subtract the offset (true) or add it (false)
- `less`: Boolean flag indicating the comparison direction (true for <=, false for >=)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (macro to extract int4 arguments)
  - PG_GETARG_BOOL (macro to extract boolean arguments)
  - pg_add_s32_overflow (overflow-safe addition function)
  - ereport (error reporting function)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - in_range_int4_int2 (delegates to this function for core logic)

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:623-656
- Part of PostgreSQL's window function range frame support
- Validates that offset is non-negative, throwing an error for negative values
- Handles integer overflow gracefully by returning appropriate boolean results
- Used in window function RANGE clauses with PRECEDING/FOLLOWING specifications
- The function is optimized for performance as noted in the source comments
- Supports cross-data-type comparisons as part of the int4/int2 function family