# finite_interval_pl

## Location
src/backend/utils/adt/timestamp.c: 3447 - 3461

## Overview
The `finite_interval_pl` function performs addition of two finite interval values with overflow protection, used internally by other interval arithmetic operations.

## Definition
```c
static void finite_interval_pl(const Interval *span1, const Interval *span2, Interval *result)
```

## Detailed Description
The `finite_interval_pl` function is a static internal helper that adds two finite (non-infinite) interval values together. It performs component-wise addition of months, days, and time fields with comprehensive overflow checking using PostgreSQL's safe arithmetic functions. The function assumes both input intervals are finite (validated by assertions) and produces an error if the result would overflow or become infinite.

The function adds corresponding fields from both intervals:
- month + month → result month
- day + day → result day  
- time + time → result time

Each addition operation uses overflow-safe functions (`pg_add_s32_overflow` for 32-bit fields, `pg_add_s64_overflow` for 64-bit time field) and checks if the resulting interval is finite.

## Parameters / Member Variables
- `span1`: First finite interval to add (input, const)
- `span2`: Second finite interval to add (input, const)  
- `result`: Output interval to store the sum (output)

## Dependencies
- Functions called/Symbols referenced:
  - `INTERVAL_NOT_FINITE` - Macro to check if an interval is infinite
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md) - Safe 32-bit integer addition with overflow detection
  - [pg_add_s64_overflow](../p/pg_add_s64_overflow.md) - Safe 64-bit integer addition with overflow detection
  - `ereport` - PostgreSQL error reporting function
- Called from (representative examples):
  - [interval_pl](../i/interval_pl.md) - Public interval addition function (src/backend/utils/adt/timestamp.c:3497)
  - [do_interval_accum](../d/do_interval_accum.md) - Interval accumulation for aggregates (src/backend/utils/adt/timestamp.c:3963)
  - [interval_avg_combine](../i/interval_avg_combine.md) - Interval average combination (src/backend/utils/adt/timestamp.c:4058)

## Notes and Other Information
- Static function, only used internally within timestamp.c
- Uses assertions to verify input intervals are finite before proceeding
- Comprehensive overflow protection for all three interval components
- Raises ERROR with ERRCODE_DATETIME_VALUE_OUT_OF_RANGE if overflow occurs or result becomes infinite
- Essential building block for PostgreSQL interval addition operations
- Designed for performance in cases where inputs are known to be finite