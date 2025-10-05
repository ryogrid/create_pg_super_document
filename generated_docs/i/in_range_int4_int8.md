# in_range_int4_int8

## Location
[src/backend/utils/adt/int.c:669-703](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L669-L703)

## Overview
A PostgreSQL function that determines whether a given int4 value falls within a range defined by a base int4 value and an int8 offset, performing all calculations in 64-bit arithmetic for accuracy.

## Definition
```c
Datum in_range_int4_int8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements range-based comparisons for PostgreSQL window functions where the offset is an int8 (64-bit integer) while the value and base are int4 (32-bit integers). To ensure arithmetic accuracy and proper overflow handling, all calculations are performed in int64 arithmetic. The function checks whether a value is within a specified range from a base point, supporting both addition and subtraction operations and either less-than-or-equal or greater-than-or-equal comparisons. It uses pg_add_s64_overflow for safe arithmetic operations and provides appropriate fallback logic when overflow occurs.

## Parameters / Member Variables
- `val`: The int4 value to test against the range (converted to int64 for calculations)
- `base`: The int4 base value that defines the center of the range (converted to int64 for calculations)
- `offset`: The int8 offset value that defines the range size (must be non-negative)
- `sub`: Boolean flag indicating whether to subtract the offset (true) or add it (false)
- `less`: Boolean flag indicating the comparison direction (true for <=, false for >=)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (macro to extract int4 arguments)
  - PG_GETARG_INT64 (macro to extract int8 argument)
  - PG_GETARG_BOOL (macro to extract boolean arguments)
  - [pg_add_s64_overflow](../p/pg_add_s64_overflow.md) (64-bit overflow-safe addition function)
  - ereport (error reporting function)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - [in_range_int2_int8](in_range_int2_int8.md) (delegates to this function for core logic)

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:669-703
- Part of PostgreSQL's window function range frame support for cross-data-type operations
- Validates that offset is non-negative, throwing an error for negative values
- All arithmetic is performed in int64 to handle large int8 offsets safely
- Handles 64-bit integer overflow gracefully using pg_add_s64_overflow
- Used in window function RANGE clauses with PRECEDING/FOLLOWING specifications involving int8 offsets
- Provides separate implementation for performance reasons as noted in source comments
- The function supports scenarios where offset values exceed int4 range but val and base are int4

## Simplified Source

```c
Datum in_range_int4_int8(PG_FUNCTION_ARGS) {
    // Convert int4 arguments to int64 for safe arithmetic
    int64 val = (int64) PG_GETARG_INT32(0);      // Value to test
    int64 base = (int64) PG_GETARG_INT32(1);     // Base point
    int64 offset = PG_GETARG_INT64(2);           // Range offset (int8)
    bool sub = PG_GETARG_BOOL(3);                // Subtract offset?
    bool less = PG_GETARG_BOOL(4);               // Less-than comparison?

    // Validate offset is non-negative
    if (offset < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE),
                       errmsg("invalid preceding or following size in window function")));

    // Apply subtraction if requested
    if (sub)
        offset = -offset;

    // Calculate range boundary with 64-bit overflow detection
    int64 range_boundary;
    if (pg_add_s64_overflow(base, offset, &range_boundary)) {
        // Handle overflow: return result based on operation direction
        PG_RETURN_BOOL(sub ? !less : less);
    }

    // Perform the range comparison
    if (less)
        PG_RETURN_BOOL(val <= range_boundary);
    else
        PG_RETURN_BOOL(val >= range_boundary);
}
```