# in_range_int4_int4

## Location
[src/backend/utils/adt/int.c:623-656](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L623-L656)

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
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md) (overflow-safe addition function)
  - ereport (error reporting function)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - [in_range_int4_int2](in_range_int4_int2.md) (delegates to this function for core logic)

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:623-656
- Part of PostgreSQL's window function range frame support
- Validates that offset is non-negative, throwing an error for negative values
- Handles integer overflow gracefully by returning appropriate boolean results
- Used in window function RANGE clauses with PRECEDING/FOLLOWING specifications
- The function is optimized for performance as noted in the source comments
- Supports cross-data-type comparisons as part of the int4/int2 function family

## Simplified Source

```c
Datum in_range_int4_int4(PG_FUNCTION_ARGS) {
    // Extract arguments
    int32 val = PG_GETARG_INT32(0);      // Value to test
    int32 base = PG_GETARG_INT32(1);     // Base point
    int32 offset = PG_GETARG_INT32(2);   // Range offset
    bool sub = PG_GETARG_BOOL(3);        // Subtract offset?
    bool less = PG_GETARG_BOOL(4);       // Less-than comparison?

    // Validate offset is non-negative
    if (offset < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE),
                       errmsg("invalid preceding or following size in window function")));

    // Apply subtraction if requested
    if (sub)
        offset = -offset;

    // Calculate range boundary with overflow detection
    int32 range_boundary;
    if (pg_add_s32_overflow(base, offset, &range_boundary)) {
        // Handle overflow: return result based on operation direction
        return PG_RETURN_BOOL(sub ? !less : less);
    }

    // Perform the range comparison
    if (less)
        PG_RETURN_BOOL(val <= range_boundary);
    else
        PG_RETURN_BOOL(val >= range_boundary);
}
```