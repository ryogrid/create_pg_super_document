# in_range_int2_int4

## Location
[src/backend/utils/adt/int.c:704-738](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L704-L738)

## Overview
A PostgreSQL function that determines whether a given int2 value falls within a range defined by a base int2 value and an int4 offset, performing calculations in 32-bit arithmetic for accuracy.

## Definition
```c
Datum in_range_int2_int4(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements range-based comparisons for PostgreSQL window functions where both the value and base are int2 (16-bit integers) but the offset is an int4 (32-bit integer). To ensure arithmetic accuracy and proper overflow handling, all calculations are performed in int32 arithmetic by converting the int2 values to int32. The function checks whether a value is within a specified range from a base point, supporting both addition and subtraction operations and either less-than-or-equal or greater-than-or-equal comparisons. It uses pg_add_s32_overflow for safe arithmetic operations and provides appropriate fallback logic when overflow occurs.

## Parameters / Member Variables
- `val`: The int2 value to test against the range (converted to int32 for calculations)
- `base`: The int2 base value that defines the center of the range (converted to int32 for calculations)
- `offset`: The int4 offset value that defines the range size (must be non-negative)
- `sub`: Boolean flag indicating whether to subtract the offset (true) or add it (false)
- `less`: Boolean flag indicating the comparison direction (true for <=, false for >=)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16 (macro to extract int2 arguments)
  - PG_GETARG_INT32 (macro to extract int4 argument)
  - PG_GETARG_BOOL (macro to extract boolean arguments)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md) (32-bit overflow-safe addition function)
  - ereport (error reporting function)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - [in_range_int2_int2](in_range_int2_int2.md) (delegates to this function for core logic)

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:704-738
- Part of PostgreSQL's window function range frame support for cross-data-type operations
- Validates that offset is non-negative, throwing an error for negative values
- All arithmetic is performed in int32 to handle large int4 offsets safely with int2 values
- Handles 32-bit integer overflow gracefully using pg_add_s32_overflow
- Used in window function RANGE clauses with PRECEDING/FOLLOWING specifications involving int4 offsets
- The function supports scenarios where offset values exceed int2 range but val and base are int2
- Converting int2 to int32 prevents overflow issues that could occur with pure int2 arithmetic

## Simplified Source

```c
Datum in_range_int2_int4(PG_FUNCTION_ARGS) {
    // Convert int2 values to int32 for safe arithmetic
    int32 val = (int32) PG_GETARG_INT16(0);
    int32 base = (int32) PG_GETARG_INT16(1);
    int32 offset = PG_GETARG_INT32(2);
    bool sub = PG_GETARG_BOOL(3);
    bool less = PG_GETARG_BOOL(4);
    int32 sum;

    // Validate offset is non-negative
    if (offset < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE),
                       errmsg("invalid preceding or following size in window function")));

    // Apply subtraction if requested
    if (sub)
        offset = -offset;

    // Perform safe addition with overflow checking
    if (unlikely(pg_add_s32_overflow(base, offset, &sum))) {
        // Handle overflow: return appropriate result based on operation direction
        PG_RETURN_BOOL(sub ? !less : less);
    }

    // Compare value against computed range boundary
    if (less)
        PG_RETURN_BOOL(val <= sum);
    else
        PG_RETURN_BOOL(val >= sum);
}
```